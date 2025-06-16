#!/usr/bin/env python3
"""
CircleCI Slack Notifier - Single message updater for deployment pipelines
"""
import os
import sys
import json
import argparse
import logging
from typing import Dict, List, Optional
import requests
from datetime import datetime

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SlackNotifier:
    """Manages Slack message updates for CircleCI pipelines"""

    def __init__(self, token: str, channel: str, workflow_id: str):
        self.token = token
        self.channel = channel
        self.workflow_id = workflow_id
        self.cache_dir = "/tmp/slack_cache"
        self.message_ts_file = f"{self.cache_dir}/message_ts_{workflow_id}.txt"
        self.phases_file = f"{self.cache_dir}/phases_{workflow_id}.json"

        # Ensure cache directory exists
        os.makedirs(self.cache_dir, exist_ok=True)

        # Color mapping
        self.colors = {
            "start": "#2196F3",  # Blue
            "progress": "#FF9800",  # Orange
            "success": "good",  # Green
            "failure": "danger"  # Red
        }

    def _load_message_ts(self) -> Optional[str]:
        """Load existing message timestamp"""
        if os.path.exists(self.message_ts_file):
            with open(self.message_ts_file, 'r') as f:
                ts = f.read().strip()
                return ts if ts and ts != "null" else None
        return None

    def _save_message_ts(self, ts: str) -> None:
        """Save message timestamp"""
        with open(self.message_ts_file, 'w') as f:
            f.write(ts)

    def _load_phases(self) -> List[Dict]:
        """Load existing phases data"""
        if os.path.exists(self.phases_file):
            with open(self.phases_file, 'r') as f:
                return json.load(f)
        return []

    def _save_phases(self, phases: List[Dict]) -> None:
        """Save phases data"""
        with open(self.phases_file, 'w') as f:
            json.dump(phases, f)

    def _update_phases(self, phase: str, status: str, step: str,
                       color_key: str, is_final: bool = False) -> List[Dict]:
        """Update phases data preserving failed states"""
        phases = self._load_phases()

        # Find existing phase
        phase_exists = False
        for p in phases:
            if p['name'] == phase:
                phase_exists = True
                # Never overwrite a failed state
                if p.get('color') != 'danger':
                    p['status'] = status
                    p['color'] = self.colors[color_key]
                    p['is_final'] = is_final
                # Always add the step
                if 'steps' not in p:
                    p['steps'] = []
                p['steps'].append(step)
                break

        # Add new phase if not exists
        if not phase_exists:
            phases.append({
                'name': phase,
                'status': status,
                'color': self.colors[color_key],
                'is_final': is_final,
                'steps': [step]
            })

        self._save_phases(phases)
        return phases

    def _build_attachments(self, phases: List[Dict]) -> List[Dict]:
        """Build Slack attachments from phases"""
        # Header attachment
        header = {
            "color": "#2196F3",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"🚀 *Infrastructure Deployment Pipeline*\n\n"
                            f"*Branch:* `{os.environ.get('CIRCLE_BRANCH', 'unknown')}` | "
                            f"*User:* `{os.environ.get('CIRCLE_USERNAME', 'unknown')}`\n"
                            f"<{os.environ.get('CIRCLE_BUILD_URL', '#')}|View Pipeline>"
                        )
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": (
                                f":gear: `{os.environ.get('CIRCLE_PROJECT_REPONAME', 'repo')}` | "
                                f":hash: Build #{os.environ.get('CIRCLE_BUILD_NUM', '0')}"
                            )
                        }
                    ]
                }
            ]
        }

        # Phase attachments (reversed for newest first)
        phase_attachments = []
        for phase in reversed(phases):
            steps_text = '\n'.join(f"• {step}" for step in phase.get('steps', []))
            attachment = {
                "color": phase['color'],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"{phase['status']}\n\n*Steps:*\n{steps_text}"
                        }
                    }
                ]
            }
            phase_attachments.append(attachment)

        return [header] + phase_attachments

    def _slack_request(self, method: str, payload: Dict) -> Dict:
        """Make Slack API request"""
        url = f"https://slack.com/api/chat.{method}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        data = response.json()
        if not data.get('ok'):
            raise Exception(f"Slack API error: {data.get('error', 'Unknown error')}")

        return data

    def update(self, phase: str, status: str, step: str,
               color: str = "progress", is_final: bool = False) -> None:
        """Update or create Slack message"""
        try:
            # Update phases
            phases = self._update_phases(phase, status, step, color, is_final)

            # Build message
            attachments = self._build_attachments(phases)
            payload = {
                "channel": self.channel,
                "attachments": attachments
            }

            # Check for existing message
            message_ts = self._load_message_ts()

            if message_ts:
                # Update existing message
                payload["ts"] = message_ts
                self._slack_request("update", payload)
                logger.info(f"Updated message: {message_ts}")
            else:
                # Create new message
                data = self._slack_request("postMessage", payload)
                message_ts = data['ts']
                self._save_message_ts(message_ts)
                logger.info(f"Created message: {message_ts}")

        except Exception as e:
            logger.error(f"Failed to update Slack: {e}")
            raise


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Update Slack deployment status')
    parser.add_argument('--phase', required=True, help='Deployment phase name')
    parser.add_argument('--status', required=True, help='Status message')
    parser.add_argument('--step', required=True, help='Step description')
    parser.add_argument('--color', default='progress',
                        choices=['start', 'progress', 'success', 'failure'],
                        help='Status color')
    parser.add_argument('--final', action='store_true', help='Mark phase as final')

    args = parser.parse_args()

    # Get environment variables
    token = os.environ.get('SLACK_ACCESS_TOKEN')
    channel = os.environ.get('SLACK_CHANNEL', 'C090S4FDHDL')
    workflow_id = os.environ.get('CIRCLE_WORKFLOW_ID')

    if not token:
        logger.error("SLACK_ACCESS_TOKEN environment variable not set")
        sys.exit(1)

    if not workflow_id:
        logger.error("CIRCLE_WORKFLOW_ID environment variable not set")
        sys.exit(1)

    # Create notifier and update
    notifier = SlackNotifier(token, channel, workflow_id)
    notifier.update(
        phase=args.phase,
        status=args.status,
        step=args.step,
        color=args.color,
        is_final=args.final
    )


if __name__ == "__main__":
    main()