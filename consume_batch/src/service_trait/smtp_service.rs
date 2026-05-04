use crate::common::*;

#[async_trait]
pub trait SmtpService: Send + Sync {
    /// Sends an HTML email to a single recipient.
    ///
    /// # Arguments
    ///
    /// * `to` - Recipient email address
    /// * `subject` - Email subject line
    /// * `html` - HTML body content
    ///
    /// # Errors
    ///
    /// Returns an error if SMTP connection, authentication, or message delivery fails.
    async fn send_html_email(&self, to: &str, subject: &str, html: &str) -> anyhow::Result<()>;

    // /// Builds per-user HTML reports and sends them to each user's email.
    // ///
    // /// Groups `summaries` by `user_id`, builds an HTML spend table for each user,
    // /// and delivers one email per user for the given year/month.
    // ///
    // /// # Arguments
    // ///
    // /// * `year` - Report year
    // /// * `month` - Report month (1–12)
    // /// * `summaries` - Aggregated spend rows returned by the MySQL monthly query
    // ///
    // /// # Errors
    // ///
    // /// Returns an error if any individual email send fails.
    // async fn send_monthly_spent_report(
    //     &self,
    //     year: i32,
    //     month: u32,
    //     summaries: &[UserMonthlySpentSummary],
    // ) -> anyhow::Result<()>;
}
