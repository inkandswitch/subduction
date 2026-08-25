# `subduction_websocket`

WebSocket transport for the Subduction sans-io driver: one complete wire
message per binary WebSocket frame, with connection pumps handed back to
the caller to spawn (the driver never schedules tasks).
