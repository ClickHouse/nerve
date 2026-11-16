# Record schemas of generic hosted sources

Byte-exact copies of the JSON Schema files of the hosted channel gateway's
contract, one for each record type and major version of a generic hosted
source: `<provider>/<record type>.v<major>.schema.json`. The gateway validates
every record that it sends against its file. Nerve reads the data tolerantly,
and its tests validate their record fixtures against these copies, so a
fixture holds only data that the gateway can send.

- `linear/linear_issue_event.v1.schema.json`: the data of `linear_issue_event`
  records, record schema `linear.issue_event/1`.

When the contract changes a file, copy it again and update its SHA-256 in
`tests/test_hosted_linear_source.py`.
