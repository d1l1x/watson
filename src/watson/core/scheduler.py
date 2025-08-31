from typing import List, Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from datetime import time
from abc import ABC, abstractmethod

class Trigger(ABC):
    """Abstract base class for all triggers."""
    
    @abstractmethod
    def to_apscheduler_trigger(self):
        """Convert to APScheduler trigger object."""
        pass

class Weekdays(Trigger):
    """Trigger that fires on weekdays (Monday-Friday) at a specific time."""
    
    def __init__(self, at: time):
        self.at = at
    
    def to_apscheduler_trigger(self):
        """Convert to APScheduler CronTrigger."""
        return CronTrigger(
            day_of_week='mon-fri',
            hour=self.at.hour,
            minute=self.at.minute,
            second=self.at.second,
        )

class Daily(Trigger):
    """Trigger that fires daily at a specific time."""
    
    def __init__(self, at: time):
        self.at = at
    
    def to_apscheduler_trigger(self):
        """Convert to APScheduler CronTrigger."""
        return CronTrigger(
            hour=self.at.hour,
            minute=self.at.minute,
            second=self.at.second,
        )

class Interval(Trigger):
    """Trigger that fires at a regular interval."""
    
    def __init__(self, seconds: int = 0, minutes: int = 0, hours: int = 0, days: int = 0):
        self.seconds = seconds
        self.minutes = minutes
        self.hours = hours
        self.days = days
    
    def to_apscheduler_trigger(self):
        """Convert to APScheduler IntervalTrigger."""
        return IntervalTrigger(
            seconds=self.seconds,
            minutes=self.minutes,
            hours=self.hours,
            days=self.days
        )


class Scheduler:
    """Scheduler that manages multiple triggers for a single function."""
    
    def __init__(self, triggers: List[Trigger] = None):
        self.scheduler = AsyncIOScheduler()
        self.triggers = triggers or []
        self.jobs = []

    async def initialize(self, func: Callable):
        """Initialize the scheduler with a function and all triggers."""
        for trigger in self.triggers:
            job = self.scheduler.add_job(
                func, 
                trigger.to_apscheduler_trigger(),
                id=f"{func.__name__}_{len(self.jobs)}"
            )
            self.jobs.append(job)
    
    def add_trigger(self, trigger: Trigger):
        """Add a new trigger to the scheduler."""
        self.triggers.append(trigger)
    
    async def start(self):
        """Start the scheduler."""
        self.scheduler.start()
    
    def shutdown(self):
        """Shutdown the scheduler."""
        self.scheduler.shutdown()
    
    def get_jobs(self):
        """Get all scheduled jobs."""
        return self.scheduler.get_jobs()
