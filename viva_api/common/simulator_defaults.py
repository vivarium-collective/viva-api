"""Default simulator repository configuration.

This module is kept separate to avoid circular imports.
"""

from viva_api.common import StrEnumBase
from viva_api.config import get_public_mode
from viva_api.simulation.models import Simulator


class SimulationConfigPublic(StrEnumBase):
    DEFAULT = "api_simulation_default.json"
    CCAM = "api_simulation_default_ccam.json"
    AWS_CDK = "api_simulation_default_aws_cdk.json"
    BASELINE = "api_simulation_ptools_ccam.json"
    VIO_WITH_MET = ""
    VIO_NO_MET = ""
    MEC = ""


class SimulationConfigPrivate(StrEnumBase):
    BASELINE = "api_simulation_default.json"
    VIO_WITH_MET = "api_test_violacein_with_metabolism.json"
    VIO_NO_MET = "api_test_violacein_no_metabolism.json"
    MEC = "api_final_mec.json"


class RepoUrl(StrEnumBase):
    VECOLI_FORK_REPO_URL = "https://github.com/vivarium-collective/vEcoli"
    VECOLI_PUBLIC_REPO_URL = "https://github.com/CovertLab/vEcoli"
    VECOLI_PRIVATE_REPO_URL = "https://github.com/CovertLabEcoli/vEcoli-private"
    # v2ecoli (process-bigraph rewrite) — runs on the Ray-on-Batch backend
    # (compute_backend_for_repo maps it to RAY). Listed here so it passes the
    # verify_simulator_payload allow-list; its config semantics differ from vEcoli
    # (no configs/ dir), so the Ray run path uses the embedded default template.
    V2ECOLI_REPO_URL = "https://github.com/vivarium-collective/v2ecoli"
    # The OFFICIAL production Ray/v2ecoli repo. Coexists with V2ECOLI_REPO_URL
    # (both -> RAY). Its name contains neither "v2ecoli" nor "vecoli", so it is
    # NOT matched by the substring fallback in compute_backend_for_repo and must be
    # listed explicitly — otherwise it silently falls back to the deployment
    # default (Batch on Stanford) and mis-routes to the vEcoli/Nextflow backend.
    SMS_ECOLI_REPO_URL = "https://github.com/CovertLabEcoli/sms-ecoli"


PUBLIC_MODE = get_public_mode()

SimulationConfigFilename: type[SimulationConfigPublic] | type[SimulationConfigPrivate] = (
    SimulationConfigPublic if PUBLIC_MODE else SimulationConfigPrivate
)
SimulationConfigFilenameType = SimulationConfigPublic | SimulationConfigPrivate


AVAILABLE_CONFIG_FILENAMES_CCAM = SimulationConfigPublic.values()
AVAILABLE_CONFIG_FILENAMES_STANFORD_DEV = SimulationConfigPrivate.values()
AVAILABLE_CONFIG_FILENAMES = AVAILABLE_CONFIG_FILENAMES_CCAM if PUBLIC_MODE else AVAILABLE_CONFIG_FILENAMES_STANFORD_DEV

DEFAULT_REPO = RepoUrl.VECOLI_FORK_REPO_URL if PUBLIC_MODE else RepoUrl.VECOLI_PRIVATE_REPO_URL
DEFAULT_BRANCH = "master"
DEFAULT_COMMIT = "6667ec1" if PUBLIC_MODE else "2f3ead"  # should be "latest"
DEFAULT_SIMULATOR = Simulator(git_commit_hash=DEFAULT_COMMIT, git_repo_url=DEFAULT_REPO, git_branch=DEFAULT_BRANCH)

# Default observables — the baseline superset covering all vEcoli analysis modules.
# Dot-separated paths that map to engine_process_reports in the simulation config.
# If no observables are specified by the user, these are used to limit output
# to only what the analysis pipeline needs (instead of emitting everything).
DEFAULT_OBSERVABLES: list[str] = [
    "bulk",
    "listeners.enzyme_kinetics.counts_to_molar",
    "listeners.fba_results.base_reaction_fluxes",
    "listeners.fba_results.external_exchange_fluxes",
    "listeners.growth_limits.ppgpp_conc",
    "listeners.mass.cell_mass",
    "listeners.mass.dna_mass",
    "listeners.mass.dry_mass",
    "listeners.mass.dry_mass_fold_change",
    "listeners.mass.instantaneous_growth_rate",
    "listeners.mass.mRna_mass",
    "listeners.mass.protein_mass",
    "listeners.mass.rRna_mass",
    "listeners.mass.rna_mass",
    "listeners.mass.smallMolecule_mass",
    "listeners.mass.tRna_mass",
    "listeners.mass.volume",
    "listeners.monomer_counts",
    "listeners.replication_data.critical_initiation_mass",
    "listeners.replication_data.critical_mass_per_oric",
    "listeners.replication_data.fork_coordinates",
    "listeners.replication_data.number_of_oric",
    "listeners.ribosome_data.actual_elongations",
    "listeners.ribosome_data.actual_prob_translation_per_transcript",
    "listeners.ribosome_data.did_initialize",
    "listeners.ribosome_data.did_terminate",
    "listeners.ribosome_data.effective_elongation_rate",
    "listeners.ribosome_data.mRNA_is_overcrowded",
    "listeners.ribosome_data.max_p",
    "listeners.ribosome_data.max_p_per_protein",
    "listeners.ribosome_data.n_ribosomes_per_transcript",
    "listeners.ribosome_data.rRNA16S_init_prob",
    "listeners.ribosome_data.rRNA16S_initiated",
    "listeners.ribosome_data.rRNA23S_init_prob",
    "listeners.ribosome_data.rRNA23S_initiated",
    "listeners.ribosome_data.rRNA5S_init_prob",
    "listeners.ribosome_data.rRNA5S_initiated",
    "listeners.ribosome_data.ribosome_init_event_per_monomer",
    "listeners.ribosome_data.target_prob_translation_per_transcript",
    "listeners.rna_counts.full_mRNA_cistron_counts",
    "listeners.rna_counts.mRNA_cistron_counts",
    "listeners.rna_counts.mRNA_counts",
    "listeners.rna_counts.partial_mRNA_counts",
    "listeners.rna_counts.partial_rRNA_counts",
    "listeners.rna_degradation_listener.count_RNA_degraded_per_cistron",
    "listeners.rna_synth_prob.actual_rna_synth_prob",
    "listeners.rna_synth_prob.gene_copy_number",
    "listeners.rna_synth_prob.max_p",
    "listeners.rna_synth_prob.target_rna_synth_prob",
    "listeners.rna_synth_prob.tu_is_overcrowded",
    "listeners.rnap_data.rna_init_event_per_cistron",
    "listeners.unique_molecule_counts.active_RNAP",
    "listeners.unique_molecule_counts.active_ribosome",
    "periplasm.global.volume",
]
