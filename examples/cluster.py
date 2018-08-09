"""An example of using cml_pipelines to run jobs on rhino's SGE cluster using
Dask-Jobqueue.

"""

from argparse import ArgumentParser
import logging
from pathlib import Path
from typing import List, Type, Union

from dask import delayed
import h5py
import numpy as np
from scipy.stats import zscore
from toolz import pipe

from cml_pipelines import Pipeline
from cmlreaders import CMLReader
from ptsa.data.filters import ButterworthFilter, MorletWaveletFilter
from ptsa.data.timeseries import TimeSeries

# Default frequencies to use for the Morlet wavelet filter
DEFAULT_FREQUENCIES = np.logspace(np.log10(6), np.log10(180), 8)

logger = logging.getLogger("zscores")
formatter = logging.Formatter("[%(levelname)s:%(asctime)s] %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)


class ZScoredPowersPipeline(Pipeline):
    """Pipeline to compute z-scored powers in parallel on the cluster.

    :param subjects: list of subjects to process

    """
    def __init__(self, subjects: List[str],
                 output_filename: Union[str, Path] = "/scratch/depalati/demo.h5",
                 morlet_freqs: np.ndarray = DEFAULT_FREQUENCIES):
        super().__init__()
        self.subjects = subjects
        self.output_filename = Path(output_filename)
        self.morlet_freqs = morlet_freqs
        self.temp_paths = []  # type: List[Path]

    @property
    def tempdir(self):
        tmp = self.output_filename.parent.joinpath("zscore")
        tmp.mkdir(exist_ok=True)
        return tmp

    def apply_filter(self, ts: TimeSeries, filter_class: Type,
                     *args, **kwargs) -> TimeSeries:
        """Apply a PTSA filter to a :class:`TimeSeries`.

        :param ts: the time series to filter
        :param filter_class: filter class type
        :param args: positional arguments to pass to the filter constructor
        :param kwargs: keyword arguments to pass to the filter constructor
        :returns: filtered time series

        """
        filt = filter_class(ts, *args, **kwargs)
        return filt.filter()

    def line_filter(self, ts: TimeSeries) -> TimeSeries:
        """Filter line noise."""
        return self.apply_filter(ts, ButterworthFilter, [58., 62.], order=4,
                                 filt_type="stop")

    def morlet_filter(self, ts: TimeSeries) -> TimeSeries:
        """Apply Morlet wavelet filter."""
        return self.apply_filter(ts, MorletWaveletFilter, self.morlet_freqs,
                                 output="power", cpus=2)

    @delayed
    def load_eeg(self, subject: str, experiment: str) -> TimeSeries:
        """Load EEG data for all sessions of the given experiment.

        :param subject: subject ID
        :param experiment: experiment name

        """
        logger.info("Loading EEG for %s/%s", subject, experiment)
        events = CMLReader.load_events(subject, experiment)
        words = events[events.type == "WORD"]
        reader = CMLReader(subject, experiment)
        eeg = reader.load_eeg(events=words, rel_start=0, rel_stop=1600)
        return eeg

    @delayed
    def timeseries_to_spectrum(self, eeg: TimeSeries) -> TimeSeries:
        """Remove line noise and apply the Morlet wavelet filter to decompose
        a time series into its frequency components.

        :param eeg: input time series
        :returns: the Morlet-decomposed data

        """
        logger.info("Applying filters...")
        return pipe(
            eeg.to_ptsa(),
            lambda d: d.add_mirror_buffer(1),
            lambda d: self.line_filter(d),
            lambda d: self.morlet_filter(d),
            lambda d: d.remove_buffer(1),
        )

    @delayed
    def spectrum_to_powers(self, powers: TimeSeries) -> np.ndarray:
        """Convert the spectrum to z-scored mean powers.

        :param powers: Morlet-decomposed data
        :returns: z-scored mean powers

        """
        logger.info("Computing zscores...")
        time_axis = [i for i, dim in enumerate(powers.dims) if dim == "time"][0]
        mean_powers = np.log10(powers.data.mean(axis=time_axis))
        zscored = zscore(mean_powers, axis=1, ddof=1)
        return zscored

    @delayed
    def save(self, subject: str, zscores: np.ndarray) -> Path:
        """Write the result of a single z-score calculation to disk."""
        path = self.tempdir.joinpath(f"{subject}.npy")
        np.save(path, zscores)
        return path

    @delayed
    def combine(self, paths: List[Path]) -> Path:
        """Combine all data into a single HDF5 file.

        :param paths: list of paths written by :meth:`save`

        """
        logger.info("Creating HDF5 file %s", str(self.output_filename))

        with h5py.File(str(self.output_filename), "w") as hfile:
            for path in paths:
                logger.info("Adding %s...", str(path))
                subject = path.name.split(".npy")[0]
                group = hfile.create_group(subject)
                group.create_dataset("zscores", data=np.load(path))

        return self.output_filename

    def cleanup(self):
        """Remove intermediate .npy files."""
        import shutil
        shutil.rmtree(self.tempdir)

    def build(self):
        experiment = "FR1"
        eegs = [self.load_eeg(subject, experiment) for subject in self.subjects]
        spectra = [self.timeseries_to_spectrum(eeg) for eeg in eegs]
        powers = [self.spectrum_to_powers(spectrum) for spectrum in spectra]
        paths = [self.save(subject, pow) for subject, pow in zip(self.subjects, powers)]
        return self.combine(paths)


def make_parser() -> ArgumentParser:
    """Setup command-line argument parsing."""
    parser = ArgumentParser()
    parser.add_argument("--local", "-l", action="store_true",
                        help="run locally (not on the cluster)")
    parser.add_argument("--visualize", "-v", action="store_true",
                        help="generate a task graph with graphviz")
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    # ix = CMLReader.get_data_index("r1")
    # fr1 = ix[ix.experiment == "FR1"]
    # subjects = np.random.choice(fr1.subject.unique(), 2)
    subjects = ["R1111M", "R1286J", "R1290M", "R1187P", "R1363T"]

    if args.local:
        cluster_kwargs = {}
    else:
        cluster_kwargs = {
            "memory": "32G",
        }

        logger.info("Setting up cluster; stdout logging won't be captured")

    # Run the pipeline
    pipeline = ZScoredPowersPipeline(subjects)

    if args.visualize:
        pipeline.visualize()

    workers = min(10, len(subjects))
    path = pipeline.run(block=True, cluster=(not args.local),
                        cluster_kwargs=cluster_kwargs, workers=workers)
    logger.info("Wrote HDF5 file to %s", str(path))
    pipeline.cleanup()
