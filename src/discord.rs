use anyhow::Result;
use serde::Serialize;
use tracing::{error, info};

const AVATAR_URL: &str =
    "https://github.com/user-attachments/assets/4513d26c-3087-42e3-8daa-09e5bba6622a";
const USERNAME: &str = "Cypress";

#[derive(Serialize, Debug)]
struct DiscordEmbed {
    title: String,
    description: String,
    color: u32,
    timestamp: String,
}

#[derive(Serialize, Debug)]
struct DiscordPayload {
    username: String,
    avatar_url: String,
    embeds: Vec<DiscordEmbed>,
}

pub struct DiscordWebhook {
    url: String,
    client: reqwest::Client,
}

impl DiscordWebhook {
    pub fn new(url: String) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn send_notification(
        &self,
        title: &str,
        description: &str,
        success: bool,
    ) -> Result<()> {
        let color = if success { 0x00FF00 } else { 0xFF0000 };
        let timestamp = chrono::Utc::now().to_rfc3339();

        let payload = DiscordPayload {
            username: USERNAME.to_string(),
            avatar_url: AVATAR_URL.to_string(),
            embeds: vec![DiscordEmbed {
                title: title.to_string(),
                description: description.to_string(),
                color,
                timestamp,
            }],
        };

        let response = self.client.post(&self.url).json(&payload).send().await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            error!("Failed to send Discord notification: {}", error_text);
            anyhow::bail!("Discord notification failed: {}", error_text);
        }

        info!("Sent Discord notification: {}", title);
        Ok(())
    }
}
