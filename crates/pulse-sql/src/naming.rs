//! Mapping between TypeScript camelCase logical field names and Postgres
//! snake_case columns. The two system fields are special-cased.

/// camelCase logical field → snake_case Postgres column.
pub fn field_to_column(field: &str) -> String {
    match field {
        "_id" => return "_id".to_string(),
        "_creationTime" => return "_creation_time".to_string(),
        _ => {}
    }
    let mut out = String::with_capacity(field.len() + 4);
    for ch in field.chars() {
        if ch.is_ascii_uppercase() {
            out.push('_');
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// snake_case Postgres column → camelCase logical field.
pub fn column_to_field(column: &str) -> String {
    match column {
        "_id" => return "_id".to_string(),
        "_creation_time" => return "_creationTime".to_string(),
        _ => {}
    }
    let mut out = String::with_capacity(column.len());
    let mut upper_next = false;
    for ch in column.chars() {
        if ch == '_' {
            upper_next = true;
        } else if upper_next {
            out.push(ch.to_ascii_uppercase());
            upper_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn camel_to_snake() {
        assert_eq!(field_to_column("channelId"), "channel_id");
        assert_eq!(field_to_column("isPrivate"), "is_private");
        assert_eq!(field_to_column("editedAt"), "edited_at");
        assert_eq!(field_to_column("body"), "body");
        assert_eq!(field_to_column("_id"), "_id");
        assert_eq!(field_to_column("_creationTime"), "_creation_time");
    }

    #[test]
    fn snake_to_camel() {
        assert_eq!(column_to_field("channel_id"), "channelId");
        assert_eq!(column_to_field("is_private"), "isPrivate");
        assert_eq!(column_to_field("body"), "body");
        assert_eq!(column_to_field("_id"), "_id");
        assert_eq!(column_to_field("_creation_time"), "_creationTime");
    }

    #[test]
    fn round_trips() {
        for f in [
            "channelId",
            "authorId",
            "editedAt",
            "isPrivate",
            "_id",
            "_creationTime",
        ] {
            assert_eq!(column_to_field(&field_to_column(f)), f);
        }
    }
}
