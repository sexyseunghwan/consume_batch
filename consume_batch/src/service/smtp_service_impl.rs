use crate::app_config::AppConfig;
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

    // /// Reads the HTML template configured via `MONTHLY_REPORT_TEMPLATE` in `.env`,
    // /// substitutes placeholders, and returns the final HTML body for one user's monthly spend report.
    // ///
    // /// # Placeholders
    // ///
    // /// | Token      | Replaced with                          |
    // /// |------------|----------------------------------------|
    // /// | `{{YEAR}}` | Report year (e.g. `2026`)              |
    // /// | `{{MONTH}}`| Report month (e.g. `4`)                |
    // /// | `{{ROWS}}` | Generated `<tr>` block per category    |
    // /// | `{{TOTAL}}`| Grand total formatted with commas      |
    // ///
    // /// # Errors
    // ///
    // /// Returns an error if `AppConfig` is not initialized or the template file cannot be read.
    // fn build_monthly_report_html(
    //     year: i32,
    //     month: u32,
    //     rows: &[&UserMonthlySpentSummary],
    // ) -> anyhow::Result<String> {
    //     let template_path: &str = AppConfig::get_global()?.monthly_report_template();
    //     let template: String = std::fs::read_to_string(template_path)
    //         .map_err(|e| anyhow!("Failed to read HTML template '{}': {}", template_path, e))?;

    //     let mut rows_html: String = String::new();
    //     let mut total: i64 = 0;

    //     for row in rows {
    //         rows_html.push_str(&format!(
    //             "<tr>\
    //                <td style='padding:8px 12px;border-bottom:1px solid #eee;'>{}</td>\
    //                <td style='padding:8px 12px;border-bottom:1px solid #eee;text-align:right;'>{} 원</td>\
    //              </tr>",
    //             row.consume_keyword_type(),
    //             format_money(*row.total_money()),
    //         ));
    //         total += row.total_money();
    //     }

    //     let html = template
    //         .replace("{{YEAR}}", &year.to_string())
    //         .replace("{{MONTH}}", &month.to_string())
    //         .replace("{{ROWS}}", &rows_html)
    //         .replace("{{TOTAL}}", &format_money(total));

    //     Ok(html)
    // }
}

/// Formats an i64 money value with thousand-separators (e.g. 1234567 → "1,234,567").
fn format_money(amount: i64) -> String {
    let s = amount.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

#[async_trait]
impl SmtpService for SmtpServiceImpl {
    /// Sends a single HTML email to `to` via the configured SMTP relay.
    async fn send_html_email(&self, to: &str, subject: &str, html: &str) -> anyhow::Result<()> {
        let email: Message = Message::builder()
            .from(self.smtp_id.parse()?)
            .to(to.parse()?)
            .subject(subject)
            .multipart(MultiPart::alternative().singlepart(SinglePart::html(html.to_string())))?;

        let creds: Credentials = Credentials::new(self.smtp_id.clone(), self.smtp_pw.clone());

        let mailer: AsyncSmtpTransport<Tokio1Executor> =
            AsyncSmtpTransport::<Tokio1Executor>::relay(&self.smtp_host)?
                .credentials(creds)
                .build();

        mailer.send(email).await.map_err(|e| anyhow!("{:?}: Failed to send email to {}", e, to))?;

        Ok(())
    }

    // /// Groups summaries by user, renders the HTML template for each user,
    // /// and sends one report email per user.
    // async fn send_monthly_spent_report(
    //     &self,
    //     year: i32,
    //     month: u32,
    //     summaries: &[UserMonthlySpentSummary],
    // ) -> anyhow::Result<()> {
    //     // Group rows by user_id.
    //     let mut by_user: HashMap<String, Vec<&UserMonthlySpentSummary>> = HashMap::new();
    //     for row in summaries {
    //         by_user.entry(row.user_id().clone()).or_default().push(row);
    //     }

    //     let subject = format!("[소비 리포트] {}년 {}월 소비 내역", year, month);

    //     // Build (email, html) pairs — template file is read once per user.
    //     let mut email_html_pairs: Vec<(String, String)> = Vec::with_capacity(by_user.len());
    //     for (email, rows) in &by_user {
    //         let html = Self::build_monthly_report_html(year, month, rows)
    //             .inspect_err(|e| {
    //                 error!(
    //                     "[SmtpServiceImpl::send_monthly_spent_report] Template error for {}: {:#}",
    //                     email, e
    //                 );
    //             })?;
    //         email_html_pairs.push((email.clone(), html));
    //     }

    //     let tasks = email_html_pairs
    //         .iter()
    //         .map(|(email, html)| self.send_html_email(email, &subject, html));

    //     let results = join_all(tasks).await;

    //     for result in results {
    //         if let Err(e) = result {
    //             error!("[SmtpServiceImpl::send_monthly_spent_report] Failed to send email: {:#}", e);
    //         }
    //     }

    //     Ok(())
    // }
}
