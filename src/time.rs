const NON_LEAP_CALENDAR: [i32; 13] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365];
const LEAP_CALENDAR: [i32; 13] = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366];

#[derive(Default)]
pub struct CDateTime {
  /// Seconds after the minute [0, 59]
  pub second: i32,
  /// Minutes after the hour  [0, 59]
  pub minute: i32,
  /// Hours since midnight [0, 23]
  pub hour: i32,
  /// Day of the month [1, 12]
  pub month_day: i32,
  /// Months since January [0, 11]
  pub month: i32,
  /// Years since 1900,
  pub year: i32,
  /// Days since Sunday [0, 6]
  pub week_day: i32,
  /// days since January 1 – [0, 365],
  pub year_day: i32,
  /// Daylight Saving Time flag. The value is Some(true) if DST is in effect, Some(false) if not and None if no information is available
  pub is_daylight_savings_time: Option<bool>,
}

impl CDateTime {

  pub fn from_f64(raw: f64) -> CDateTime {
    let mut datetime = CDateTime::default();
    let mut q: i32;

    if raw < 0.0 || raw > 1e6 /* About 2700 AD*/ {
      // TODO: investigate
      return datetime;
    }

    let mut year: i32 = 1;
    let mut day: i32 = raw as i32;
    let time: i32 = ((raw - day as f64) * 86400.0 + 0.5) as i32;
    datetime.hour = time / 3600;
    datetime.minute = (time / 60) % 60;
    datetime.second = time % 60;

    day += 693593; /* Days from 1/1/1 to 12/31/1899 */
    datetime.week_day = (day + 1) % 7;

    q = day / 146097;  /* 146097 days in 400 years */
    year += 400 * q;
    day -= q * 146097;

    q = day / 36524;  /* 36524 days in 100 years */
    if q > 3 {
      q = 3;
    }
    year += 100 * q;
    day -= q * 36524;

    q = day / 1461;  /* 1461 days in 4 years */
    year += 4 * q;
    day -= q * 1461;

    q = day / 365;  /* 365 days in 1 year */
    if q > 3 {
      q = 3;
    }
    year += q;
    day -= q * 365;

    let cal = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { &LEAP_CALENDAR } else { &NON_LEAP_CALENDAR };
    datetime.month = 0;
    while datetime.month < 12 {
      if day < cal[datetime.month as usize + 1] {
        break;
      }
      datetime.month += 1;
    }
    datetime.year = year - 1900;
    datetime.month_day = day - cal[datetime.month as usize] + 1;
    datetime.year_day = day;
    datetime.is_daylight_savings_time = None;
    datetime
  }
}