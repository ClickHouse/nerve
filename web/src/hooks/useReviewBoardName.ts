import { useEffect, useState } from 'react';
import { codeReview } from '../api/codeReview';

export function useReviewBoardName() {
  const [name, setName] = useState('Tasks Code Review');
  useEffect(() => {
    let active = true;
    const load = async () => {
      try {
        const status = await codeReview.status();
        if (active && status.board_name) setName(status.board_name);
      } catch { /* An older backend retains the default navigation label. */ }
    };
    void load();
    return () => { active = false; };
  }, []);
  return name;
}
