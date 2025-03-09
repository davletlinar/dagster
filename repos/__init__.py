from dagster import Definitions

from .r_resources import (
    role_postgres,
    megafon_postgres
)

from .megafon import(
    megafon_assets,
    megafon_automation_sensor,
    megafon_job,
    megafon_schedule
)

from .role import(
    role_assets,
    role_job,
    role_schedule,
    stores_retry_job
    )

role_jobs = [role_job, stores_retry_job]

assets = [*megafon_assets, *role_assets]
resources = {
    'role_postgres': role_postgres(),
    'megafon_postgres': megafon_postgres(),}
sensors = [megafon_automation_sensor,]
jobs = [*role_jobs, megafon_job]
schedules = [role_schedule, megafon_schedule]

defs = Definitions(
    assets=assets,
    resources=resources,
    sensors=sensors,
    jobs=jobs,
    schedules=schedules,
    )