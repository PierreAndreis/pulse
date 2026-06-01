//! Server-sent-events transport. Each client gets a channel plus a ring buffer
//! so a reconnect with `Last-Event-ID` replays the missed tail (at-least-once),
//! falling back to a `resync` event when the buffer has rolled past. Built in M2.
