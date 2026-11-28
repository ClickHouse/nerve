# Generated image delivery

An inline Codex `imageGeneration` result appears as an image/download card in
the web conversation. When the originating turn posts its answer to an
attachment-capable channel, it also sends the image to that conversation.
Telegram receives a document. Turns configured to reply through an explicit
answer tool keep the card in the session unless the agent explicitly shares
the file.

Only bounded inline PNG, JPEG and WebP data is imported. Image inspection
outputs and subagent work never trigger automatic delivery. Native file
paths and file IDs are left to the explicit file flow.

The validated image is saved under `<workspace>/output/generated-images/`
using session and content hashes. The attachment upload uses the validated
bytes directly. Conversation history stores only the file card, and duplicate
generation completions within a turn do not resend the attachment. The
generated directory is ignored by Git, including in workspaces with older
config-repo templates. An image is not written if Git reports that its path
is tracked or cannot verify the exclusion.

An import or upload failure appears in the web conversation while the turn
continues. After an upload failure, the saved image is still available from
its file card. A failed send is left for explicit follow-up because the
remote channel may already have received it.
