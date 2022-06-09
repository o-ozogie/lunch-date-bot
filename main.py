from datetime import date
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from time import sleep, time
from dataclasses import dataclass
from functools import reduce
import json
import math
import random
from slack_sdk import WebClient


client = WebClient(token=os.environ['APP_SECRET'])
scheduler = BlockingScheduler()

@dataclass
class Event:
    name: str
    started_at: str
    channel: str
    join_groups: list[str]
    trigger_weekday: int
    trigger_hour: int
    trigger_minute: int
    limit_type: str
    limit: int
    week_interval: int

def get_events(path: str) -> list[Event]:
    get_event = lambda parsed : Event(
        name=parsed['name'],
        started_at=parsed['started_at'],
        channel=parsed['channel'],
        join_groups=parsed['join_groups'],
        trigger_weekday=parsed['trigger_weekday'],
        trigger_hour=parsed['trigger_hour'],
        trigger_minute=parsed['trigger_minute'],
        limit_type=parsed['limit_type'],
        limit=parsed['limit'],
        week_interval=parsed['week_interval']
    )
    with open(path, 'r') as events:
        return list(map(get_event, json.loads(events.read())))

@dataclass
class Group:
    name: str
    member_ids: list[str]

def get_groups(join_groups: list[str]) -> list[Group]:
    groups = []
    for usergroup in client.usergroups_list(include_users=True)['usergroups']:
        if usergroup['handle'] in join_groups:
            groups.append(Group(name=usergroup['handle'], member_ids=usergroup['users']))
    return groups

def process(event: Event):
    if ((date.today() - date.strftime(event.started_at, '%Y-%m-%d')).days / 7) % event.week_interval != 0:
        return
    message = ask_absentee(event)
    sleep(3600)
    notice_lunch_group(event, message)

def ask_absentee(event: Event) -> dict[str, str]: # return channel_id, ts
    mentions = ''
    for group in event.join_groups:
        mentions += f'@{group} '
    response = client.chat_postMessage(
        channel=event.channel,
        mrkdwn=True,
        link_names=True,
        text=f'*{event.name}*, 불참자는 스레드로 알려주세요!' + '\n' + mentions
    )
    return [response['channel'], response['ts']]

def notice_lunch_group(event: Event, absentee_message: list[str, str]):
    notice = f'*{event.name}* 런데 조입니다~' + '\n'
    groups = get_groups(event.join_groups)
    absentee_ids = [message['user'] for message in client.conversations_replies(channel=absentee_message[0], ts=absentee_message[1])['messages'] if 'user' in message]
    for group in get_lunch_groups(event=event, groups=groups, absentee_ids=absentee_ids):
        notice += '- ' + ', '.join(list(map(lambda user_id: f'<@{user_id}>', group))) + '\n'
    client.chat_postMessage(channel=event.channel, mrkdwn=True, text=notice)

def get_lunch_groups(event: Event, groups: list[Group], absentee_ids: list[str]) -> list[list[str]]:    
    def shuffle(array: list) -> list:
        random.shuffle(array)
        return array
    attendee_groups = list(map(lambda group: shuffle([member_id for member_id in group.member_ids if member_id not in absentee_ids]), groups))
    return list(map(lambda group: shuffle(group), distribute(event=event, attendee_groups=attendee_groups)))

def distribute(event: Event, attendee_groups: list[list[str]]) -> list[list[str]]:
    if event.limit_type == 'group_count':
        return group_count_distribute(attendee_groups=attendee_groups, limit=event.limit)
    if event.limit_type == 'group_size':
        return group_size_distribute(attendee_groups=attendee_groups, limit=event.limit)

def group_count_distribute(attendee_groups: list[list[str]], limit: int) -> list[list[str]]:
    groups = [[] for _ in range(limit)]

    member_count = 0
    for attendee_group in attendee_groups:
        for member_id in attendee_group:
            groups[member_count % limit].append(member_id)
            member_count += 1
    
    return groups

def group_size_distribute(attendee_groups: list[list[str]], limit: int) -> list[list[str]]:
    attendee_count = reduce(lambda count, attendee_group: count + len(attendee_group), attendee_groups, 0)
    groups = [[] for _ in range(math.ceil(attendee_count / limit))]

    x, y = 0, 0
    member_count = 0
    while attendee_count != member_count:
        if len(attendee_groups[x % len(attendee_groups)]) <= y:
            x += 1
            continue
        groups[int(member_count / limit)].append(attendee_groups[x % len(attendee_groups)][y])
        x += 1
        member_count += 1
        if x % len(attendee_groups) == 0:
            y += 1

    return groups


if __name__ == '__main__':
    for event in get_events('./resources/events.json'):
        scheduler.add_job(
            process,
            trigger='cron',
            hour=event.trigger_hour,
            minute=event.trigger_minute,
            day_of_week=event.trigger_weekday,
            args=[event],
            id=str(time())
        )
    scheduler.start()
