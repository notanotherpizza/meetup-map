from worker.platforms.base import Platform
from worker.platforms.luma import LumaPlatform
from worker.platforms.meetup import MeetupPlatform

PLATFORMS: list[Platform] = [MeetupPlatform(), LumaPlatform()]


def get_platform(url: str) -> Platform:
    for platform in PLATFORMS:
        if platform.can_handle(url):
            return platform
    raise ValueError(f"No platform can handle URL: {url}")
