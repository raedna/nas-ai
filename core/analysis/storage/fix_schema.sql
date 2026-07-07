CREATE TABLE IF NOT EXISTS analysis_sessions (
    id BIGSERIAL PRIMARY KEY,

    analyzer_type TEXT NOT NULL DEFAULT 'FIX Message',
    analysis_mode TEXT NOT NULL,

    source_type TEXT,
    source_name TEXT,

    summary TEXT,
    warning_count INTEGER NOT NULL DEFAULT 0,
    message_count INTEGER NOT NULL DEFAULT 0,
    group_count INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analysis_messages (
    id BIGSERIAL PRIMARY KEY,

    session_id BIGINT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,

    message_index INTEGER NOT NULL,
    group_key TEXT,
    group_label TEXT,

    raw_text TEXT NOT NULL,
    summary TEXT,

    msg_type TEXT,
    msg_seq_num TEXT,
    sender TEXT,
    target TEXT,
    sending_time TEXT,
    transact_time TEXT,

    cl_ord_id TEXT,
    order_id TEXT,
    secondary_order_id TEXT,
    exec_id TEXT,
    exec_type TEXT,
    ord_status TEXT,

    symbol TEXT,
    security_id TEXT,
    security_id_source TEXT,
    side TEXT,

    order_qty TEXT,
    last_qty TEXT,
    last_px TEXT,
    avg_px TEXT,
    cum_qty TEXT,
    leaves_qty TEXT,

    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    business_object JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (session_id, message_index)
);

CREATE TABLE IF NOT EXISTS analysis_message_tags (
    id BIGSERIAL PRIMARY KEY,

    message_id BIGINT NOT NULL REFERENCES analysis_messages(id) ON DELETE CASCADE,

    position_index INTEGER NOT NULL,

    tag TEXT NOT NULL,
    tag_name TEXT,

    value TEXT,
    value_name TEXT,
    value_description TEXT,
    description TEXT,

    tag_status TEXT,
    tag_warning TEXT,

    has_enums BOOLEAN,
    enum_valid TEXT,
    enum_warning TEXT,

    ocr_original_tag TEXT,
    ocr_tag_repaired BOOLEAN NOT NULL DEFAULT FALSE,
    ocr_repair_warning TEXT,
    ocr_inferred BOOLEAN NOT NULL DEFAULT FALSE,
    ocr_score TEXT,

    source TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_analysis_sessions_created_at
    ON analysis_sessions(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_analysis_messages_session_id
    ON analysis_messages(session_id);

CREATE INDEX IF NOT EXISTS idx_analysis_messages_group_key
    ON analysis_messages(group_key);

CREATE INDEX IF NOT EXISTS idx_analysis_messages_cl_ord_id
    ON analysis_messages(cl_ord_id);

CREATE INDEX IF NOT EXISTS idx_analysis_messages_order_id
    ON analysis_messages(order_id);

CREATE INDEX IF NOT EXISTS idx_analysis_messages_secondary_order_id
    ON analysis_messages(secondary_order_id);

CREATE INDEX IF NOT EXISTS idx_analysis_messages_exec_id
    ON analysis_messages(exec_id);

CREATE INDEX IF NOT EXISTS idx_analysis_messages_msg_seq_num
    ON analysis_messages(msg_seq_num);

CREATE INDEX IF NOT EXISTS idx_analysis_messages_symbol
    ON analysis_messages(symbol);

CREATE INDEX IF NOT EXISTS idx_analysis_messages_security_id
    ON analysis_messages(security_id);

CREATE INDEX IF NOT EXISTS idx_analysis_message_tags_message_id
    ON analysis_message_tags(message_id);

CREATE INDEX IF NOT EXISTS idx_analysis_message_tags_tag
    ON analysis_message_tags(tag);

CREATE INDEX IF NOT EXISTS idx_analysis_message_tags_tag_value
    ON analysis_message_tags(tag, value);