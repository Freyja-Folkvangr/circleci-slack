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
import urllib.request
import urllib.error
from datetime import datetime

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
        # Each phase gets its own file to avoid conflicts
        self.phase_file_pattern = f"{self.cache_dir}/phase_{workflow_id}_*.json"

        os.makedirs(self.cache_dir, exist_ok=True)

        self.colors = {
            "start": "#2196F3",
            "progress": "#FF9800",
            "success": "#4CAF50",
            "failure": "#F44336"
        }

    def _load_message_ts(self) -> Optional[str]:
        """Load existing message timestamp"""
        try:
            if os.path.exists(self.message_ts_file):
                with open(self.message_ts_file, 'r') as f:
                    ts = f.read().strip()
                    return ts if ts and ts != "null" else None
        except (IOError, OSError) as e:
            logger.warning(f"Failed to load message timestamp: {e}")
        return None

    def _save_message_ts(self, ts: str) -> None:
        """Save message timestamp"""
        try:
            with open(self.message_ts_file, 'w') as f:
                f.write(ts)
        except (IOError, OSError) as e:
            logger.error(f"Failed to save message timestamp: {e}")

    def _load_phases(self) -> List[Dict]:
        """Load all existing phases from separate files"""
        import glob
        phases = []

        try:
            phase_files = glob.glob(f"{self.cache_dir}/phase_{self.workflow_id}_*.json")
            for file_path in phase_files:
                try:
                    with open(file_path, 'r') as f:
                        phase_data = json.load(f)
                        phases.append(phase_data)
                except (IOError, OSError, json.JSONDecodeError) as e:
                    logger.warning(f"Failed to load phase file {file_path}: {e}")
                    continue
        except Exception as e:
            logger.warning(f"Failed to load phases: {e}")

        return phases

    def _save_phase(self, phase_name: str, phase_data: Dict) -> None:
        """Save individual phase data to separate file"""
        phase_file = f"{self.cache_dir}/phase_{self.workflow_id}_{phase_name}.json"
        try:
            with open(phase_file, 'w') as f:
                json.dump(phase_data, f, indent=2)
        except (IOError, OSError) as e:
            logger.error(f"Failed to save phase {phase_name}: {e}")

    def _save_phases(self, phases: List[Dict]) -> None:
        """Legacy method - not used anymore"""
        pass

    def _update_phases(self, phase: str, status: str, step: str,
                       color_key: str, is_final: bool = False) -> List[Dict]:
        """Update single phase data and return all phases"""
        color = self.colors.get(color_key, self.colors["progress"])

        # Load existing phase data for this specific phase
        phase_file = f"{self.cache_dir}/phase_{self.workflow_id}_{phase}.json"
        current_phase = None

        try:
            if os.path.exists(phase_file):
                with open(phase_file, 'r') as f:
                    current_phase = json.load(f)
        except (IOError, OSError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to load existing phase {phase}: {e}")

        # Update or create phase data
        if current_phase:
            # Never overwrite a failed state
            if current_phase.get('color') != self.colors["failure"]:
                current_phase['status'] = status
                current_phase['color'] = color
                current_phase['is_final'] = is_final

            # Always add the step
            if 'steps' not in current_phase:
                current_phase['steps'] = []
            current_phase['steps'].append(step)
        else:
            # Create new phase
            current_phase = {
                'name': phase,
                'status': status,
                'color': color,
                'is_final': is_final,
                'steps': [step]
            }

        # Save this phase
        self._save_phase(phase, current_phase)

        # Return all phases for message construction
        return self._load_phases()

    def _build_attachments(self, phases: List[Dict]) -> List[Dict]:
        """Build Slack attachments from phases"""
        branch = os.environ.get('CIRCLE_BRANCH', 'unknown')
        username = os.environ.get('CIRCLE_USERNAME', 'unknown')
        build_url = os.environ.get('CIRCLE_BUILD_URL', '#')
        repo_name = os.environ.get('CIRCLE_PROJECT_REPONAME', 'repo')
        build_num = os.environ.get('CIRCLE_BUILD_NUM', '0')

        header = {
            "color": "#2196F3",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"🚀 *Infrastructure Deployment Pipeline*\n\n"
                            f"*Branch:* `{branch}` | *User:* `{username}`\n"
                            f"<{build_url}|View Pipeline>"
                        )
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f":gear: `{repo_name}` | :hash: Build #{build_num}"
                        }
                    ]
                }
            ]
        }

        # Sort phases by a predefined order for consistent display
        phase_order = ["initialization", "validation", "infrastructure", "cleanup", "application", "destruction"]
        sorted_phases = []

        for phase_name in phase_order:
            for phase in phases:
                if phase.get('name') == phase_name:
                    sorted_phases.append(phase)
                    break

        # Add any phases not in the predefined order
        for phase in phases:
            if phase.get('name') not in phase_order:
                sorted_phases.append(phase)

        # Create attachments (newest first - reverse the order)
        phase_attachments = []
        for phase in reversed(sorted_phases):
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
        """Make Slack API request using urllib"""
        url = f"https://slack.com/api/chat.{method}"
        data = json.dumps(payload).encode('utf-8')

        req = urllib.request.Request(
            url,
            data=data,
            headers={
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = json.loads(response.read().decode('utf-8'))

            if not response_data.get('ok'):
                error_msg = response_data.get('error', 'Unknown error')
                raise Exception(f"Slack API error: {error_msg}")

            return response_data

        except urllib.error.URLError as e:
            logger.error(f"HTTP request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Slack API request failed: {e}")
            raise

    def update(self, phase: str, status: str, step: str,
               color: str = "progress", is_final: bool = False) -> None:
        """Update or create Slack message"""
        try:
            phases = self._update_phases(phase, status, step, color, is_final)
            attachments = self._build_attachments(phases)

            payload = {
                "channel": self.channel,
                "attachments": attachments
            }

            message_ts = self._load_message_ts()

            if message_ts:
                payload["ts"] = message_ts
                self._slack_request("update", payload)
                logger.info(f"Updated Slack message: {message_ts}")
            else:
                data = self._slack_request("postMessage", payload)
                message_ts = data['ts']
                self._save_message_ts(message_ts)
                logger.info(f"Created Slack message: {message_ts}")

        except Exception as e:
            logger.error(f"Failed to update Slack notification: {e}")
            sys.exit(1)


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

    token = os.environ.get('SLACK_ACCESS_TOKEN')
    channel = os.environ.get('SLACK_CHANNEL', 'C090S4FDHDL')
    workflow_id = os.environ.get('CIRCLE_WORKFLOW_ID')

    if not token:
        logger.error("SLACK_ACCESS_TOKEN environment variable not set")
        sys.exit(1)

    if not workflow_id:
        logger.error("CIRCLE_WORKFLOW_ID environment variable not set")
        sys.exit(1)

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