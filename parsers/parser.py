import uuid
import pandas as pd


def first(*values):
    """Return the first non-None value, or None if all are None."""
    for value in values:
        if value is not None:
            return value
    return None


class Span:

    def __init__(self):

        self.span_id = str(uuid.uuid4())

        self.trace_id = None

        self.parent_id = None

        self.depth = 0

        self.event_time = None

        self.event_action = None

        self.method_name = None

        self.duration_ms = None

        self.result_status = None

        self.error_message = None

        self.error_stack = None

        self.request_id = None

        self.user_id = None

        self.user_tracking_id = None

        self.content_id = None

        self.service_name = "cobrand"

        self.attributes = {}

        self.details = {}

        self.children = []        # <-- NEW

    def get_detail(self, key, default=None):

        return self.details.get(key, default)

    def get_attribute(self, key, default=None):

        return self.attributes.get(key, default)


class TraceParser:

    def __init__(self):

        self.rows = []

    def parse(self, trace, trace_timestamp=None):

        self.rows = []

        trace_id = str(uuid.uuid4())

        self._walk(trace, trace_id, trace_timestamp=trace_timestamp)

        return self.rows

    def _walk(self, node, trace_id, parent=None, depth=0, trace_timestamp=None):

        span = Span()

        span.trace_id = trace_id

        span.depth = depth

        if parent:
            span.parent_id = parent.span_id

        span.event_action = node.get("event.action")

        span.method_name = node.get("method.name")

        # A node's own "timestamp" key has never been observed in real
        # data (confirmed directly: 0 of 8,836 span_fact rows had one)
        # — only the outer log line, one level above trace.method,
        # carries a timestamp for the whole trace. node.get("timestamp")
        # is kept as the first choice in case that ever changes upstream
        # (e.g. a future app version starts stamping individual spans),
        # falling back to the trace-wide timestamp passed down from the
        # root otherwise. This means every span in a trace currently
        # shares one timestamp (the trace's start time), not a precise
        # per-span time — an approximation, not a data loss to hide.
        span.event_time = first(
            node.get("timestamp"),
            trace_timestamp,
        )

        span.duration_ms = node.get("event.duration_ms")

        result = node.get("result") or {}

        error = node.get("error") or {}

        attributes = node.get("attributes") or {}

        details = node.get("details") or {}

        span.result_status = result.get("result.status")

        # BUG FIX: this used to read error.get("error.message") /
        # error.get("error.stack") — but the "error" object's actual
        # keys are "message" and "stack" (e.g. "error":{"message":
        # "ffprobe exited...","stack":"Error: ffprobe exited..."}).
        # error.get("error.message") can never succeed against that
        # shape, since no such key exists — this was silently
        # discarding every single error/stack trace in the warehouse
        # (confirmed directly: 0 non-null error_message across every
        # row, regardless of event_action).
        #
        # Separately, this application is inconsistent about *where*
        # it puts error info: some spans carry a genuine "error"
        # object (e.g. cobrand.pdf.generate), while others have no
        # "error" key at all but instead carry a flattened copy as
        # literal dotted keys directly inside "details" (e.g.
        # cobrand.job.process's details containing "error.message"/
        # "error.stack" keys). first() checks all three shapes in
        # order, so whichever one a given span actually uses, the
        # error still gets captured.
        span.error_message = first(
            error.get("message"),
            error.get("error.message"),
            details.get("error.message"),
        )

        span.error_stack = first(
            error.get("stack"),
            error.get("error.stack"),
            details.get("error.stack"),
        )

        span.attributes = attributes

        span.details = details

        span.request_id = first(
            details.get("request.id"),
            attributes.get("request.id")
        )

        span.user_id = first(
            details.get("user.id"),
            attributes.get("user.id")
        )

        span.user_tracking_id = first(
            details.get("user_tracking.id"),
            attributes.get("user_tracking.id")
        )

        span.content_id = first(
            details.get("content.id"),
            attributes.get("content.id")
        )

        if parent:
            parent.children.append(span)
        self.rows.append(span)

        for child in node.get("children", []):

            self._walk(
                child,
                trace_id,
                parent=span,
                depth=depth + 1,
                trace_timestamp=trace_timestamp
            )

    def to_dataframe(self):

        records = []

        for span in self.rows:

            records.append({

                "trace_id": span.trace_id,

                "span_id": span.span_id,

                "parent_id": span.parent_id,

                "depth": span.depth,

                "event_time": span.event_time,

                "event_action": span.event_action,

                "method_name": span.method_name,

                "duration_ms": span.duration_ms,

                "result_status": span.result_status,

                "error_message": span.error_message,

                "error_stack": span.error_stack,

                "request_id": span.request_id,

                "user_id": span.user_id,

                "user_tracking_id": span.user_tracking_id,

                "content_id": span.content_id,

                "service_name": span.service_name,

                "attributes": span.attributes,

                "details": span.details

            })

        return pd.DataFrame(records)

if __name__ == "__main__":

    print("Parser loaded successfully.")
