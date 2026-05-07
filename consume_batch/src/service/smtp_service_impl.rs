use crate::common::*;
use crate::service_trait::smtp_service::SmtpService;

/// SMTP email service implementation.
///
/// Wraps SMTP credentials and delivers HTML emails via the `lettre` crate
/// using an async Tokio transport.
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct SmtpServiceImpl {
    smtp_host: String,
    smtp_id: String,
    smtp_pw: String,
}

impl SmtpServiceImpl {
    /// Creates a new `SmtpServiceImpl` from explicit credentials.
    pub fn new(smtp_host: impl Into<String>, smtp_id: impl Into<String>, smtp_pw: impl Into<String>) -> Self {
        Self {
            smtp_host: smtp_host.into(),
            smtp_id: smtp_id.into(),
            smtp_pw: smtp_pw.into(),
        }
    }
}

#[async_trait]
impl SmtpService for SmtpServiceImpl {
    /// Sends a single HTML email to `to` via the configured SMTP relay.
    async fn send_html_email(&self, to: &str, subject: &str, html: &str) -> anyhow::Result<()> {
        if self.smtp_host.is_empty() || self.smtp_id.is_empty() || self.smtp_pw.is_empty() {
            return Err(anyhow!(
                "SMTP credentials not configured. Set SMTP_HOST, SMTP_ID, SMTP_PW in .env"
            ));
        }

        let email: Message = Message::builder()
            .from(
                self.smtp_id
                    .parse()
                    .map_err(|e| anyhow!("Invalid SMTP_ID '{}': {}", self.smtp_id, e))?,
            )
            .to(to
                .parse()
                .map_err(|e| anyhow!("Invalid recipient address '{}': {}", to, e))?)
            .subject(subject)
            .multipart(MultiPart::alternative().singlepart(SinglePart::html(html.to_string())))
            .map_err(|e| anyhow!("Failed to build email message: {}", e))?;

        let creds: Credentials = Credentials::new(self.smtp_id.clone(), self.smtp_pw.clone());

        let mailer: AsyncSmtpTransport<Tokio1Executor> =
            AsyncSmtpTransport::<Tokio1Executor>::relay(&self.smtp_host)
                .map_err(|e| anyhow!("Failed to connect to SMTP relay '{}': {}", self.smtp_host, e))?
                .credentials(creds)
                .build();

        mailer
            .send(email)
            .await
            .map_err(|e| anyhow!("SMTP send failed to '{}': {}", to, e))?;

        Ok(())
    }
}
