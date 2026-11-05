"""Durable, idempotent manual authorizations. Mutations share the ledger lock."""

import uuid

from nerve.utils.time import utc_now_iso


class CodeReviewRequests:
    async def cr_request_replay(self, pr_id, actor_id, key, version_id):
        row = await self.cr_one(
            "SELECT * FROM code_review_requests WHERE pr_id=? AND actor_id=? AND idempotency_key=?",
            (pr_id, actor_id, key),
        )
        if row and row["version_id"] != version_id:
            raise ValueError("Ключ запроса уже использован для другой версии")
        return row

    async def cr_manual_eligibility(self, pr_id, version_id, day, version_cap, daily_cap):
        pr = await self.cr_one("SELECT * FROM code_review_prs WHERE id=?", (pr_id,))
        if not pr or pr["current_version"] != version_id:
            return "Версия PR изменилась. Обновите страницу и запустите проверку снова"
        settings = await self.cr_one("SELECT * FROM code_review_profiles WHERE id=?", (pr["profile"],))
        if not settings or settings["paused"]:
            return "Профиль на полной паузе"
        if pr["state"] != "open":
            return "PR закрыт"
        if pr["draft"]:
            return "Черновики не проверяются"
        if pr["error"]:
            return "Не удалось обновить PR. Дождитесь успешного обновления"
        version = await self.cr_one("SELECT * FROM code_review_versions WHERE id=?", (version_id,))
        if not version:
            return "Версия PR недоступна"
        if version["status"] == "complete" or pr["reviewed_version"] == version_id:
            return "Эта версия уже проверена"
        if version["status"] == "awaiting_validation":
            return "Отчёт сохранён; ожидается подтверждение актуальности без модели"
        if await self.cr_one(
            "SELECT a.run_id FROM code_review_attempts a JOIN code_review_versions v ON v.id=a.version_id "
            "WHERE v.pr_id=? AND a.settled=0", (pr_id,),
        ):
            return "Проверяется"
        used = await self.cr_one(
            "SELECT COALESCE(SUM(MAX(spent+uncertain,reserved*(1-settled))),0) used "
            "FROM code_review_attempts WHERE version_id=?", (version_id,),
        )
        if used["used"] >= version_cap - 0.001:
            return "Лимит расходов на эту версию исчерпан"
        if await self.cr_day_spend(day) >= daily_cap - 0.001:
            return "Суточный бюджет исчерпан или зарезервирован. Нужен новый запрос после освобождения бюджета"
        return ""

    async def cr_request_review(self, profile, pr_id, version_id, actor_id, key, day, version_cap, daily_cap):
        async with self._atomic():
            replay = await self.cr_request_replay(pr_id, actor_id, key, version_id)
            if replay:
                return replay
            # Distinct clicks/tabs share one active authorization for this PR.
            active = await self.cr_one(
                "SELECT * FROM code_review_requests WHERE pr_id=? AND state IN ('pending','claimed')", (pr_id,),
            )
            if active:
                raise ValueError("Проверка уже запрошена. Обновите страницу")
            reason = await self.cr_manual_eligibility(pr_id, version_id, day, version_cap, daily_cap)
            request_id, now = "crr-" + uuid.uuid4().hex, utc_now_iso()
            await self.db.execute(
                """INSERT INTO code_review_requests
                (id,profile,pr_id,version_id,actor_id,idempotency_key,day,state,reason,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (request_id, profile, pr_id, version_id, actor_id, key, day,
                 "rejected" if reason else "pending", reason, now, now),
            )
            if not reason:
                await self.db.execute("UPDATE code_review_prs SET content_updated_at=? WHERE id=?", (now, pr_id))
            return await self.cr_one("SELECT * FROM code_review_requests WHERE id=?", (request_id,))

    async def cr_cancel_requests(self, profile, reason, request_id=None):
        await self._write(
            "UPDATE code_review_requests SET state='cancelled',reason=?,updated_at=? "
            "WHERE profile=? AND state IN ('pending','claimed') AND (? IS NULL OR id=?)",
            (reason, utc_now_iso(), profile, request_id, request_id),
        )

    async def cr_expire_requests(self, profile, day):
        await self._write(
            "UPDATE code_review_requests SET state='cancelled',reason=?,updated_at=? "
            "WHERE profile=? AND state='pending' AND day!=?",
            ("Запрос истёк при смене суток. Запустите проверку снова", utc_now_iso(), profile, day),
        )
