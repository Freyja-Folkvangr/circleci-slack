#!/usr/bin/env python3

import os
import sys
import json
import argparse
import logging
import time
from typing import Dict, List, Optional
import urllib.request
import urllib.error

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SlackAPIError(Exception):
    pass


class SlackNotifier:

    def __init__(self, token: str, channel: str, workflow_id: str, storage_channel_name: str):
        self.token = token
        self.channel = channel
        self.workflow_id = workflow_id
        self.storage_channel_name = storage_channel_name
        self.storage_channel_id = None
        self.pipeline_title = "Infrastructure Deployment Pipeline"

        self.colors = {
            "start": "#2196F3",
            "progress": "#FF9800",
            "success": "#4CAF50",
            "failure": "#F44336",
            "skipped": "#9E9E9E"
        }

        self._resolve_storage_channel_id()

    def _resolve_storage_channel_id(self) -> None:

        try:
            payload = {'types': 'public_channel,private_channel', 'limit': 200}
            response = self._slack_request('conversations.list', payload)

            for channel in response.get('channels', []):
                if channel['name'] == self.storage_channel_name:
                    self.storage_channel_id = channel['id']
                    return

            cursor = response.get('response_metadata', {}).get('next_cursor')
            while cursor:
                payload['cursor'] = cursor
                response = self._slack_request('conversations.list', payload)

                for channel in response.get('channels', []):
                    if channel['name'] == self.storage_channel_name:
                        self.storage_channel_id = channel['id']
                        return

                cursor = response.get('response_metadata', {}).get('next_cursor')

            raise SlackAPIError(f"Channel '{self.storage_channel_name}' not found")

        except Exception as e:
            logger.error(f"Failed to resolve storage channel: {e}")
            raise

    def _slack_request(self, method: str, payload: Dict) -> Dict:

        url = f"https://slack.com/api/{method}"

        if method in ['chat.postMessage', 'chat.update']:
            data = json.dumps(payload).encode('utf-8')
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
        else:
            data = urllib.parse.urlencode(payload).encode('utf-8')
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

        req = urllib.request.Request(url, data=data, headers=headers)

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = json.loads(response.read().decode('utf-8'))

            if not response_data.get('ok'):
                error_msg = response_data.get('error', 'Unknown error')
                raise SlackAPIError(f"Slack API error: {error_msg}")

            return response_data

        except urllib.error.URLError as e:
            logger.error(f"HTTP request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Slack API request failed: {e}")
            raise

    def _get_all_storage_messages(self) -> List[Dict]:

        all_messages = []
        try:
            payload = {'channel': self.storage_channel_id, 'limit': 200}

            while True:
                response = self._slack_request('conversations.history', payload)
                messages = response.get('messages', [])
                all_messages.extend(messages)

                cursor = response.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
                payload['cursor'] = cursor

        except Exception as e:
            logger.error(f"Failed to get storage messages: {e}")

        return all_messages

    def _cleanup_and_find_pipeline(self) -> Optional[Dict]:

        messages = self._get_all_storage_messages()
        current_time = time.time()
        ten_hours_ago = current_time - (10 * 60 * 60)

        pipeline_msg = None
        to_delete = []

        for msg in messages:
            try:
                if not msg.get('bot_id'):
                    continue

                msg_time = float(msg['ts'])

                if msg_time < ten_hours_ago:
                    to_delete.append(msg['ts'])
                    continue

                data = json.loads(msg['text'])
                msg_workflow_id = data.get('workflow_id')

                if msg_workflow_id == self.workflow_id:
                    if not pipeline_msg or msg_time > float(pipeline_msg['ts']):
                        if pipeline_msg:
                            to_delete.append(pipeline_msg['ts'])
                        pipeline_msg = {
                            'data': data,
                            'ts': msg['ts']
                        }
                    else:
                        to_delete.append(msg['ts'])

            except (json.JSONDecodeError, ValueError, KeyError):
                continue

        for ts in to_delete:
            try:
                self._slack_request('chat.delete', {
                    'channel': self.storage_channel_id,
                    'ts': ts
                })
                logger.info(f"Deleted old/duplicate message: {ts}")
            except Exception as e:
                logger.warning(f"Failed to delete message {ts}: {e}")

        return pipeline_msg

    def _get_pipeline_data(self) -> Dict:

        pipeline_msg = self._cleanup_and_find_pipeline()

        if pipeline_msg:
            data = pipeline_msg['data'].copy()
            data['_storage_ts'] = pipeline_msg['ts']
            logger.info(f"Found existing pipeline data with storage_ts: {pipeline_msg['ts']}")
            return data

        logger.info("No existing pipeline data found, creating new")
        return {
            'workflow_id': self.workflow_id,
            'phases': [],
            'message_ts': None,
            'created_at': time.time(),
            '_storage_ts': None
        }

    def _save_pipeline_data(self, data: Dict) -> None:

        storage_ts = data.get('_storage_ts')
        save_data = {k: v for k, v in data.items() if k != '_storage_ts'}
        save_data['timestamp'] = time.time()

        payload = {
            'channel': self.storage_channel_id,
            'text': json.dumps(save_data)
        }

        if storage_ts:
            payload['ts'] = storage_ts
            self._slack_request('chat.update', payload)
            logger.info(f"Updated existing storage message: {storage_ts}")
        else:
            response = self._slack_request('chat.postMessage', payload)
            new_ts = response['ts']
            data['_storage_ts'] = new_ts
            logger.info(f"Created new storage message: {new_ts}")

    def _update_phase_data(self, data: Dict, phase: str, status: str, step: str,
                           color_key: str, is_final: bool) -> None:

        color = self.colors.get(color_key, self.colors["progress"])
        phases = data.get('phases', [])

        phase_found = False
        for p in phases:
            if p['name'] == phase:
                phase_found = True

                if p.get('color') != self.colors["failure"]:
                    p['status'] = status
                    p['color'] = color
                    p['is_final'] = is_final

                if 'steps' not in p:
                    p['steps'] = []

                if step not in p['steps']:
                    p['steps'].append(step)

                p['last_updated'] = time.time()
                break

        if not phase_found:
            current_time = time.time()
            phases.append({
                'name': phase,
                'status': status,
                'color': color,
                'is_final': is_final,
                'steps': [step],
                'started_at': current_time,
                'last_updated': current_time
            })

        data['phases'] = phases

    def _build_message_attachments(self, data: Dict) -> List[Dict]:

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
                            f"🚀 *{self.pipeline_title}*\n\n"
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

        phases = data.get('phases', [])
        sorted_phases = sorted(phases, key=lambda x: x.get('started_at', 0))

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

    def update(self, phase: str, status: str, step: str,
               color: str = "progress", is_final: bool = False) -> None:

        try:
            logger.info(f"Starting update for phase: {phase}, workflow: {self.workflow_id}")

            pipeline_data = self._get_pipeline_data()

            self._update_phase_data(pipeline_data, phase, status, step, color, is_final)

            self._save_pipeline_data(pipeline_data)

            attachments = self._build_message_attachments(pipeline_data)

            payload = {
                "channel": self.channel,
                "attachments": attachments
            }

            message_ts = pipeline_data.get('message_ts')

            if message_ts:
                payload["ts"] = message_ts
                self._slack_request("chat.update", payload)
                logger.info(f"Updated main message: {message_ts}")
            else:
                response = self._slack_request("chat.postMessage", payload)
                message_ts = response['ts']
                pipeline_data['message_ts'] = message_ts
                self._save_pipeline_data(pipeline_data)
                logger.info(f"Created main message: {message_ts}")

        except Exception as e:
            logger.error(f"Failed to update Slack notification: {e}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Update Slack deployment status')
    parser.add_argument('--phase', required=True, help='Deployment phase name')
    parser.add_argument('--status', required=True, help='Status message')
    parser.add_argument('--step', required=True, help='Step description')
    parser.add_argument('--color', default='progress',
                        choices=['start', 'progress', 'success', 'failure', 'skipped'],
                        help='Status color')
    parser.add_argument('--final', action='store_true', help='Mark phase as final')
    parser.add_argument('--title', default='Infrastructure Deployment Pipeline',
                        help='Pipeline title for Slack message')

    args = parser.parse_args()

    token = os.environ.get('SLACK_ACCESS_TOKEN')
    channel = os.environ.get('SLACK_CHANNEL', 'C090S4FDHDL')
    storage_channel_name = os.environ.get('SLACK_STORAGE_CHANNEL')
    workflow_id = os.environ.get('CIRCLE_WORKFLOW_ID')

    if not token:
        logger.error("SLACK_ACCESS_TOKEN environment variable not set")
        sys.exit(1)

    if not workflow_id:
        logger.error("CIRCLE_WORKFLOW_ID environment variable not set")
        sys.exit(1)

    if not storage_channel_name:
        logger.error("SLACK_STORAGE_CHANNEL environment variable not set")
        sys.exit(1)

    notifier = SlackNotifier(token, channel, workflow_id, storage_channel_name)
    notifier.pipeline_title = args.title
    notifier.update(
        phase=args.phase,
        status=args.status,
        step=args.step,
        color=args.color,
        is_final=args.final
    )


if __name__ == "__main__":
    main()