pub fn round_to(ts: i64, to: i64) -> i64 {
    ts.div_euclid(to).checked_mul(to).unwrap_or(ts)
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn utc_millis(ts: &str) -> i64 {
        Utc.datetime_from_str(ts, "%F %H:%M")
            .unwrap()
            .timestamp_millis()
    }

    fn from_utc_millis(ts: i64) -> String {
        Utc.timestamp_millis(ts).format("%F %H:%M").to_string()
    }

    fn round_utc_to(ts: &str, to: i64) -> String {
        from_utc_millis(round_to(utc_millis(ts), to))
    }

    #[test]
    fn test_overflow() {
        assert_eq!(i64::MIN, round_to(i64::MIN, 10));
    }

    #[test]
    #[rustfmt::skip]
    fn test_round_utc_to() {
        assert_eq!("1972-01-01 22:00", &round_utc_to("1972-01-01 22:00", 60 * 60 * 1000));
        assert_eq!("1972-01-01 22:00", &round_utc_to("1972-01-01 22:33", 60 * 60 * 1000));
        assert_eq!("1972-01-01 22:00", &round_utc_to("1972-01-01 22:59", 60 * 60 * 1000));
        assert_eq!("1972-01-01 00:00", &round_utc_to("1972-01-01 22:00", 24 * 60 * 60 * 1000));
        assert_eq!("1972-01-01 00:00", &round_utc_to("1972-01-01 00:00", 24 * 60 * 60 * 1000));

        assert_eq!("1962-01-01 22:00", &round_utc_to("1962-01-01 22:00", 60 * 60 * 1000));
        assert_eq!("1962-01-01 22:00", &round_utc_to("1962-01-01 22:33", 60 * 60 * 1000));        
        assert_eq!("1962-01-01 22:00", &round_utc_to("1962-01-01 22:59", 60 * 60 * 1000));
        assert_eq!("1962-01-01 00:00", &round_utc_to("1962-01-01 22:32", 24 * 60 * 60 * 1000));
        assert_eq!("1962-01-01 00:00", &round_utc_to("1962-01-01 00:00", 24 * 60 * 60 * 1000));
    }
}