# MCP gateway catalog fixtures

The JSON files in this directory are the shared fixtures of the MCP gateway's
catalog contract, schema version 2. They are byte-for-byte copies of the
contract's test data, which the gateway's own tests generate and check. Do not
edit them here. After a change of the contract, copy all files again and run
`tests/test_mcp_gateway.py`.

| File | Case |
| --- | --- |
| `catalog.json` | Two servers, each with a current inventory. |
| `catalog-unknown-inventory.json` | Two servers with an unknown inventory: one that the gateway never read, and one whose first read failed. |
| `catalog-empty-inventory.json` | A server whose upstream served a list without a tool. |
| `catalog-stale-after-failure.json` | A server whose last read failed after an earlier read succeeded. |
| `catalog-older-desired-revision.json` | A server whose policy changed after the gateway's last read. |
| `catalog-no-tools-yet.json` | A server whose tool policy names no tool yet. |
| `catalog-all-tools.json` | A server with the tool mode `all`: no tool names, and an inventory of the tools that the gateway served. |
| `catalog-review-changes.json` | A server whose entry reviews tool changes (`changes` is `review`). |
| `catalog-no-servers.json` | An agent without a server. |
| `catalog-vectors.json` | Catalogs that a consumer accepts (`accept`) and rejects (`reject`, one broken rule each; `field` names the member). |
