from apache_beam import DoFn, pvalue
from asgarde.failure import Failure

from team_league.domain.team_stats import TeamStats
from team_league.domain.team_stats_raw import TeamStatsRaw

FAILURES = 'failures'


class TeamStatsRawFieldsValidationFn(DoFn):

    def __init__(self, pipeline_step: str, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.pipeline_step = pipeline_step

    def process(self, element, *args, **kwargs):
        try:
            team_stats_raw: TeamStatsRaw = element

            yield team_stats_raw.validate_fields()
        except Exception as err:
            failure = Failure(
                pipeline_step=self.pipeline_step,
                input_element=str(element),
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


class TeamStatsMapperFn(DoFn):

    def __init__(self, pipeline_step: str, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.pipeline_step = pipeline_step

    def process(self, element, *args, **kwargs):
        try:
            team_stats_raw: TeamStatsRaw = element

            yield TeamStats.computeTeamStats(team_stats_raw)
        except Exception as err:
            failure = Failure(
                pipeline_step=self.pipeline_step,
                input_element=str(element),
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


class TeamStatsWithSloganFn(DoFn):

    def __init__(self, pipeline_step: str, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.pipeline_step = pipeline_step

    def process(self, element, *args, **kwargs):
        try:
            team_stats: TeamStats = element

            yield team_stats.add_slogan_to_stats()
        except Exception as err:
            failure = Failure(
                pipeline_step=self.pipeline_step,
                input_element=str(element),
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)
