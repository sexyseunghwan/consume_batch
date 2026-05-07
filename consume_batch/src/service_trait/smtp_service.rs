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
}
