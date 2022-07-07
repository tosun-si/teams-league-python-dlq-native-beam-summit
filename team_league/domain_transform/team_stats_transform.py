from typing import Tuple

import apache_beam as beam
from apache_beam import PCollection, ParDo
from asgarde.failure import Failure

from team_league.domain.team_stats import TeamStats
from team_league.domain.team_stats_raw import TeamStatsRaw
from team_league.domain_transform.team_stats_mapper_do_fns import FAILURES, TeamStatsRawFieldsValidationFn, \
    TeamStatsMapperFn, TeamStatsWithSloganFn

VALIDATE_FIELDS = 'Validate raw fields'
COMPUTE_TEAM_STATS = 'Compute team stats'
ADD_SLOGAN_TEAM_STATS = 'Add slogan to team stats'


class TeamStatsTransform(beam.PTransform):
    def __init__(self):
        super().__init__()

    def expand(self, inputs: PCollection[TeamStatsRaw]) -> \
            Tuple[PCollection[TeamStats], PCollection[Failure]]:
        # When.
        outputs_map1, failures_map1 = (
                inputs | VALIDATE_FIELDS >> ParDo(TeamStatsRawFieldsValidationFn(VALIDATE_FIELDS))
                .with_outputs(FAILURES, main='outputs')
        )

        outputs_map2, failures_map2 = (
                outputs_map1 | COMPUTE_TEAM_STATS >> ParDo(TeamStatsMapperFn(COMPUTE_TEAM_STATS))
                .with_outputs(FAILURES, main='outputs')
        )

        final_outputs, failures_map3 = (
                outputs_map2 | ADD_SLOGAN_TEAM_STATS >> ParDo(TeamStatsWithSloganFn(ADD_SLOGAN_TEAM_STATS))
                .with_outputs(FAILURES, main='outputs')
        )

        result_all_failures = (
                (failures_map1, failures_map2, failures_map3)
                | 'All Failures PCollections' >> beam.Flatten()
        )

        return final_outputs, result_all_failures
