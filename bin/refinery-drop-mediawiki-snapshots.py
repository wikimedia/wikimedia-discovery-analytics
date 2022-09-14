#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Note: You should make sure to put refinery/python on your PYTHONPATH.
#   export PYTHONPATH=$PYTHONPATH:/path/to/refinery/python

"""
Automatically drops old partitions from the mediawiki raw and historical
tables, and tables derived from them, that keep the same snapshot structure.
See AFFECTED_TABLES dict for a comprehensive list.

As this data sets are historical (they span from the beginning of time
to latest import), the dimension used to determine which partitions need
to be removed is not time, it's "snapshot". The number of snapshot kept is
6 for all tables except it is 2 for mediawiki_wikitext_history (huge dataset).

Note: Ad-hoc snapshots not following the default naming convention
snapshot=YYYY-MM[-DD], like private snapshots, are not considered neither
affected by this script.

Usage: refinery-drop-mediawiki-snapshots [options]

Options:
    -h --help                       Show this help message and exit.
    -v --verbose                    Turn on verbose debug logging.
    -n --dry-run                    Don't actually drop any partitions, just output Hive queries.
"""


from docopt import docopt
from refinery.hive import Hive, HivePartition
from refinery.hdfs import Hdfs
import datetime
import logging
import os
import re
import sys


# Set up logging to be split:
#   INFO+DEBUG+WARNING -> stdout
#   ERROR              -> stderr
# Thanks to https://stackoverflow.com/users/5124424/zoey-greer
class LessThanFilter(logging.Filter):
    def __init__(self, exclusive_maximum, name=""):
        super(LessThanFilter, self).__init__(name)
        self.max_level = exclusive_maximum

    def filter(self, record):
        #non-zero return means we log this message
        return 1 if record.levelno < self.max_level else 0

logger = logging.getLogger()
logger.setLevel(logging.NOTSET)

formatter = logging.Formatter(
    fmt='%(asctime)s %(levelname)-6s %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)

handler_out = logging.StreamHandler(sys.stdout)
handler_out.setLevel(logging.DEBUG)
handler_out.addFilter(LessThanFilter(logging.ERROR))
handler_out.setFormatter(formatter)
logger.addHandler(handler_out)

handler_err = logging.StreamHandler(sys.stderr)
handler_err.setLevel(logging.ERROR)
handler_err.setFormatter(formatter)
logger.addHandler(handler_err)


# Tables that have snapshots to be managed
# key: database, value: (key: table, value: keep_snapshots)
AFFECTED_TABLES = {
    'discovery': {
        'all_subgraphs': 4,
        'top_subgraph_items': 4,
        'top_subgraph_triples': 4,
    }
}

# Tables partitioned by wiki in addition to by snapshot
WIKI_DB_TABLES = [
    'all_subgraphs',
    'top_subgraph_items',
    'top_subgraph_triples',
]


# Returns the partitions to be dropped given a hive table
def get_partitions_to_drop(hive, table, keep_snapshots):
    logger.debug('Getting partitions to drop...')
    partitions = hive.partition_specs(table)

    # For tables partitioned by dimensions other than snapshot
    # extract just the snapshot spec:
    # snapshot=2017-01,wiki=enwiki => snapshot=2017-01
    if table in WIKI_DB_TABLES:
        snapshots = set([])
        for partition in partitions:
            snapshot = partition.split(Hive.partition_spec_separator)[0]
            snapshots.add(snapshot)
        partitions = list(snapshots)

    # Filter out ad-hoc or private snapshots
    partitions = [
        p for p in partitions
        if re.match("^snapshot=[0-9]{4}[0-9]{2}([0-9]{2})?$", p)
    ]

    # Select partitions to drop (keep the most recent <keep_snapshots> ones)
    partitions.sort(reverse = True)
    partitions_to_drop = partitions[keep_snapshots:]

    return partitions_to_drop

# Drop given hive table partitions (if dry_run, just print)
def drop_partitions(hive, table, partitions, dry_run):
    if partitions:
        # HACK: For tables partitioned by dimensions other than snapshot
        # add <dimension>!='' to snapshot spec, so that Hive deletes
        # the whole snapshot partition with all sub-partitions in it.
        if table in WIKI_DB_TABLES:
            partitions = [
                Hive.partition_spec_separator.join([p, "wiki!=''"])
                for p in partitions
            ]

        logger.info(
            'Dropping {0} partitions from {1}.{2}'
            .format(len(partitions), hive.database, table))
        for partition in partitions:
            logger.debug('\t{0}'.format(partition))
        if not dry_run:
            hive.drop_partitions(table, partitions)
    else:
        logger.info(
            'No partitions need to be dropped from {0}.{1}'
            .format(hive.database, table)
        )

if __name__ == '__main__':
    # Parse arguments
    arguments = docopt(__doc__)
    verbose         = arguments['--verbose']
    dry_run         = arguments['--dry-run']

    # Setup logging level
    logger.setLevel(logging.INFO)
    if verbose:
        logger.setLevel(logging.DEBUG)
    logger.debug("Running in VERBOSE mode")

    if dry_run:
        logger.info("Running in DRY-RUN mode")

    for database, tables_and_keep_snapshots in AFFECTED_TABLES.items():
        # Instantiate Hive
        hive = Hive(database)

        # Apply the cleaning to each table
        for table, keep_snapshot in tables_and_keep_snapshots.items():
            logger.debug('Processing table {0} keeping {1} snapshots'.format(table, keep_snapshot))
            partitions = get_partitions_to_drop(hive, table, keep_snapshot)
            drop_partitions(hive, table, partitions, dry_run)
