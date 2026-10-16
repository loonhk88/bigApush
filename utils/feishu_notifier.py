"""飞书机器人通知"""
import json
import logging
import requests

logger = logging.getLogger(__name__)


class FeishuNotifier:
    def __init__(self, webhook_url: str = "", security_keyword: str = ""):
        self.webhook_url = webhook_url
        self.security_keyword = security_keyword

    def send_text(self, text: str) -> bool:
        if not self.webhook_url:
            logger.warning("飞书 webhook_url 未配置")
            return False
        
        # 处理安全关键词 - 在消息开头添加安全关键词
        if self.security_keyword:
            # 如果消息已经包含安全关键词，就不重复添加
            if self.security_keyword not in text:
                text = f"{self.security_keyword}\n{text}"
        
        payload = {"msg_type": "text", "content": {"text": text}}
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            result = resp.json()
            if result.get("code") == 0:
                logger.info("飞书推送成功")
                return True
            else:
                logger.error("飞书推送失败: %s", result)
                return False
        except Exception as e:
            logger.error("飞书推送异常: %s", e)
            return False

    def send_post(self, title: str, content_lines: list) -> bool:
        if not self.webhook_url:
            logger.warning("飞书 webhook_url 未配置")
            return False
        
        # 处理安全关键词 - 在标题中添加安全关键词
        if self.security_keyword:
            # 如果标题已经包含安全关键词，就不重复添加
            if self.security_keyword not in title:
                title = f"{self.security_keyword} | {title}"
        
        post_content = []
        for line in content_lines:
            post_content.append([{"tag": "text", "text": line}])
        
        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": post_content
                    }
                }
            }
        }
        
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            result = resp.json()
            if result.get("code") == 0:
                logger.info("飞书推送成功")
                return True
            else:
                logger.error("飞书推送失败: %s", result)
                return False
        except Exception as e:
            logger.error("飞书推送异常: %s", e)
            return False
