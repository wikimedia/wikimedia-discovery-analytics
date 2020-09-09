"""
Fetch classification thresholds from ORES

Queries the ores public API to determine the most appropriate thresholds to use when indexing
predictions into elasticsearch. Based off sample code from aaron halfaker in
https://gist.github.com/halfak/630dc3fd811995c2a0260d43da462645
"""

from argparse import ArgumentParser
import json
import logging
import requests
import sys
from typing import cast, Any, Dict, Mapping, NamedTuple, Optional, Sequence


# Version of ORES api we know how to talk to
PATH = "/v3/scores"

# label precision targets in decreasing preference order.
PRECISION_TARGETS = [0.7, 0.5, 0.3, 0.15]

# Default threshold to use if none of the precision targets can be satisfied
DEFAULT_THRESHOLD = 0.9

Config = NamedTuple('Config', [
    ('wiki', str),
    ('model', str),
    ('ores_scores_for_context_api', str),
])


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('--model', required=True)
    parser.add_argument('--output-path', required=True)
    parser.add_argument('--ores-host', default='https://ores.wikimedia.org')
    return parser


def main(
    model: str,
    output_path: str,
    ores_host: str
) -> int:
    ores_scores_api = ores_host + PATH
    thresholds = get_all_thresholds(model, ores_scores_api)
    with open(output_path, 'wt') as f:
        json.dump(thresholds, f)

    return 0


def get_supported_wikis(model: str, ores_scores_api: str) -> Sequence[str]:
    """Retrieve the set of all wikis the model supports"""
    doc = requests.get(ores_scores_api).json()
    return [wiki for wiki, meta in doc.items() if model in meta['models']]


def get_labels(config: Config) -> Sequence[str]:
    """Retrieve the set of all possible labels from ORES api"""
    doc = requests.get(
        config.ores_scores_for_context_api,
        params={
            'models': config.model,
            'model_info': 'params'
        }
    ).json()

    return doc[config.wiki]['models'][config.model]['params']['labels']


def get_threshold_at_precision(
    config: Config, label: str, target: float
) -> Optional[Mapping[str, Any]]:
    """Retrieve a threshold that will meet the precision target from ORES api"""
    doc = requests.get(
        config.ores_scores_for_context_api,
        params={
            'models': config.model,
            'model_info': "statistics.thresholds.{0}.'maximum recall @ precision >= {1}'".format(repr(label), target)
        }
    ).json()

    statistics = doc[config.wiki]['models'][config.model]['statistics']
    thresholds = statistics['thresholds'][label]

    if len(thresholds) == 1 and thresholds[0] is not None:
        return thresholds[0]
    else:
        return None


def get_all_thresholds(model: str, ores_scores_api: str) -> Mapping[str, Mapping[str, float]]:
    """Assemble prediction thresholds for all labels of configured model"""
    label_thresholds = cast(Dict[str, Dict[str, float]], {})
    for wiki in get_supported_wikis(model, ores_scores_api):
        label_thresholds[wiki] = {}
        config = Config(wiki, model, ores_scores_api + '/' + wiki)
        for label in get_labels(config):
            for target in PRECISION_TARGETS:
                optimization = get_threshold_at_precision(config, label, target)
                if optimization is not None and optimization['recall'] >= 0.5:
                    label_thresholds[wiki][label] = optimization['threshold']
                    break
            else:
                label_thresholds[wiki][label] = DEFAULT_THRESHOLD
    return label_thresholds


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
