import asyncio
import gzip
import io
import os
import sys
import tarfile
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from enum import StrEnum
from pathlib import Path

import httpx
from httpx import AsyncClient
from tqdm import tqdm

from viva_api.analysis.models import AnalysisRun, ExperimentAnalysisDTO, OutputFile, TsvOutputFile
from viva_api.common.simulator_defaults import SimulationConfigFilename
from viva_api.simulation.models import (
    HpcRun,
    ParcaDataset,
    RepoDiscovery,
    Simulation,
    SimulationRun,
    Simulator,
    SimulatorVersion,
)


class BaseUrl(StrEnum):
    RKE_PROD = "https://sms.cam.uchc.edu"
    RKE_DEV = "https://sms-dev.cam.uchc.edu"
    LOCAL = "http://localhost:8888"
    RKE_PROD_FORWARDED = "http://localhost:8000"
    RKE_DEV_FORWARDED = "http://localhost:1111"
    STANFORD_FORWARDED = "http://localhost:8080"
    STANFORD_DEV_FORWARDED = "http://localhost:62505"
    LOCAL_8080 = "http://localhost:8080"


DEFAULT_BASE_URL = BaseUrl.LOCAL_8080
DEFAULT_REQUEST_TIMEOUT = 1000

SUPPORTED_CONFIGS = [name.replace(".json", "") for name in SimulationConfigFilename.values()]


def _parse_content_disposition_filename(header_value: str) -> str | None:
    """Return the ``filename`` parameter from a Content-Disposition header, or None.

    Accepts both the quoted form (``attachment; filename="foo.tar.gz"``) and the
    unquoted form (``attachment; filename=foo.tar.gz``).  The RFC 6266 ``filename*``
    form is not supported because the server emits plain ASCII filenames.
    """
    if not header_value:
        return None
    for part in header_value.split(";"):
        kv = part.strip()
        if not kv.lower().startswith("filename="):
            continue
        value = kv[len("filename=") :].strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        return value or None
    return None


@asynccontextmanager
async def async_client(base_url: BaseUrl, timeout: int = 300) -> AsyncIterator[AsyncClient]:
    try:
        async with AsyncClient(base_url=base_url, timeout=timeout) as client:
            yield client
    finally:
        pass


class E2EDataService:
    base_url: BaseUrl
    client: httpx.Client

    def __init__(self, base_url: BaseUrl, timeout: int = 300) -> None:
        self.base_url = base_url
        self.client = httpx.Client(base_url=self.base_url, timeout=timeout)

    # -- Simulator --

    def get_simulator(self) -> SimulatorVersion:
        latest = self.submit_get_latest_simulator()
        uploaded = self.submit_upload_simulator(simulator=latest)
        status = "pending"
        try:
            while status not in ["completed", "failed"]:
                status = self.submit_get_simulator_build_status(simulator=uploaded)
                time.sleep(1.0)
        except Exception as e:
            raise httpx.HTTPError("Could not set up the simulator. Try again.") from e
        return uploaded

    def get_simulator_status(self, simulator_id: int) -> str:
        return self.submit_get_simulator_status(simulator_id=simulator_id)

    # -- Simulation --

    def run_workflow(
        self,
        params: httpx.QueryParams | None = None,
        experiment_id: str | None = None,
        simulator_id: int | None = None,
        config_filename: str | None = None,
        num_generations: int | None = None,
        num_seeds: int | None = None,
        description: str | None = None,
        run_parameter_calculator: bool | None = None,
        observables: list[str] | None = None,
        analysis_options: dict[str, object] | None = None,
        ecoli_sources_uri: str | None = None,
        ecoli_sources_overlays: str | None = None,
        ecoli_sources_repo_url: str | None = None,
        ecoli_sources_ref: str | None = None,
        tags: list[str] | None = None,
    ) -> Simulation:
        simulation = self.submit_run_workflow(
            params=params,
            config_filename=config_filename,
            experiment_id=experiment_id,
            simulator_id=simulator_id,
            num_generations=num_generations,
            num_seeds=num_seeds,
            description=description,
            run_parameter_calculator=run_parameter_calculator,
            observables=observables,
            analysis_options=analysis_options,
            ecoli_sources_uri=ecoli_sources_uri,
            ecoli_sources_overlays=ecoli_sources_overlays,
            ecoli_sources_repo_url=ecoli_sources_repo_url,
            ecoli_sources_ref=ecoli_sources_ref,
            tags=tags,
        )
        return simulation

    def tag_workflow(self, simulation_id: int, tags: list[str]) -> Simulation:
        return self.submit_add_tags(simulation_id=simulation_id, tags=tags)

    def get_workflow(self, simulation_id: int) -> Simulation:
        return self.submit_get_workflow(simulation_id=simulation_id)

    def show_workflows(self, experiment_id: str | None = None, tag: str | None = None) -> list[Simulation]:
        sims = self.submit_list_workflows(experiment_id=experiment_id, tag=tag)
        return sorted(sims, key=lambda s: s.database_id)

    def show_simulators(self) -> list[SimulatorVersion]:
        return self.submit_list_simulators()

    def discover_repo(self, simulator_id: int) -> RepoDiscovery:
        try:
            response = self.client.get("/api/v1/simulations/discovery", params={"simulator_id": simulator_id})
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return RepoDiscovery(**response.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not discover repo contents for simulator {simulator_id}") from e

    def get_workflow_log(self, simulation_id: int, truncate: bool = True) -> str:
        return self.submit_get_workflow_log(simulation_id=simulation_id, truncate=truncate)

    def get_workflow_status(self, simulation_id: int) -> SimulationRun:
        return self.submit_get_workflow_status(simulation_id=simulation_id)

    def cancel_workflow(self, simulation_id: int) -> SimulationRun:
        return self.submit_cancel_workflow(simulation_id=simulation_id)

    def run_analysis(self, simulation_id: int, modules: str | None = None) -> dict:  # type: ignore[type-arg]
        """Run standalone analysis on existing simulation output."""
        params: dict[str, str] = {}
        if modules:
            params["modules"] = modules
        response = self.client.post(url=f"/api/v1/simulations/{simulation_id}/analysis", params=params)
        if response.status_code != 200:
            raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")
        return response.json()  # type: ignore[no-any-return]

    def get_output_data_sync(self, simulation_id: int, dest: Path) -> Path:
        """Download simulation outputs synchronously (no async event loop required)."""
        simulation = self.submit_get_workflow(simulation_id=simulation_id)
        experiment_id = simulation.experiment_id
        output_path = dest / f"{experiment_id}.tar.gz"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.client.stream("POST", f"/api/v1/simulations/{simulation_id}/data") as response:
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}")
            with open(output_path, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
        with tarfile.open(output_path, "r:gz") as tar:
            tar.extractall(output_path.parent)  # noqa: S202
        # .stem on .tar.gz gives "foo.tar", so strip both suffixes
        return output_path.parent / experiment_id

    async def get_output_data(self, simulation_id: int, dest: Path | None = None, timeout: int = 1800) -> Path:
        if dest is None:
            dest = Path(os.getcwd()).absolute()
        archive_path = await self.submit_stream_output_data(
            simulation_id=simulation_id, output_dirpath=dest, timeout=timeout
        )
        if not isinstance(archive_path, Path):
            raise TypeError()
        with tarfile.open(archive_path, "r:gz") as tar:
            # Extract to parent dir - the tar already contains the experiment_id directory
            tar.extractall(archive_path.parent)  # noqa: S202
        # Return the extracted directory (archive stem is the experiment_id)
        extracted_dir = archive_path.parent / archive_path.stem
        return extracted_dir

    # -- Parca --

    def get_parca_datasets(self) -> list[ParcaDataset]:
        return self.submit_get_parca_datasets()

    def get_parca_status(self, parca_id: int) -> HpcRun:
        return self.submit_get_parca_status(parca_id=parca_id)

    # -- Analysis --

    def get_analysis(self, analysis_id: int) -> ExperimentAnalysisDTO:
        return self.submit_get_analysis(analysis_id=analysis_id)

    def get_analysis_status(self, analysis_id: int) -> AnalysisRun:
        return self.submit_get_analysis_status(analysis_id=analysis_id)

    def get_analysis_log(self, analysis_id: int) -> str:
        return self.submit_get_analysis_log(analysis_id=analysis_id)

    def get_analysis_plots(self, analysis_id: int) -> list[OutputFile]:
        return self.submit_get_analysis_plots(analysis_id=analysis_id)

    # -- Low-level HTTP methods: Simulator --

    def submit_get_latest_simulator(self, repo_url: str | None = None, branch: str | None = None) -> Simulator:
        try:
            from viva_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO

            latest_response = self.client.get(
                url="/core/v1/simulator/latest",
                params={"git_branch": branch or DEFAULT_BRANCH, "git_repo_url": repo_url or DEFAULT_REPO},
            )
            return Simulator(**latest_response.json())
        except Exception as e:
            raise httpx.HTTPError(
                f"Could not get the latest simulator from the repo {repo_url} on branch {branch}"
            ) from e

    def submit_upload_simulator(self, simulator: Simulator, force: bool = False) -> SimulatorVersion:
        try:
            params = {"force": "true"} if force else {}
            uploaded_response = self.client.post(
                url="/core/v1/simulator/upload", json=simulator.model_dump(), params=params
            )
            uploaded_response.raise_for_status()
            return SimulatorVersion(**uploaded_response.json())
        except httpx.HTTPStatusError as e:
            raise httpx.HTTPError(f"Could not build the simulator: {e.response.status_code} — {e.response.text}") from e
        except Exception as e:
            raise httpx.HTTPError(f"Could not build the simulator: {simulator.model_dump()}") from e

    def submit_list_simulators(self) -> list[SimulatorVersion]:
        try:
            simulators = self.client.get(url="/core/v1/simulator/versions")
            if simulators.status_code != 200:
                raise httpx.HTTPError(f"Server returned {simulators.status_code}: {simulators.text}")  # noqa: TRY301
            return [SimulatorVersion(**sim) for sim in simulators.json()["versions"]]
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not list simulators: {e}") from e

    def submit_get_simulator_build_status(self, simulator: SimulatorVersion) -> str:
        try:
            status_update_response = self.client.get(
                url="/core/v1/simulator/status", params={"simulator_id": simulator.database_id}
            )
            if status_update_response.status_code != 200:
                raise httpx.HTTPError(  # noqa: TRY301
                    f"Server returned {status_update_response.status_code}: {status_update_response.text}"
                )
            return status_update_response.json().get("status", "")  # type: ignore[no-any-return]
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not fetch build status for simulator {simulator.database_id}: {e}") from e

    def submit_get_simulator_build_status_full(self, simulator_id: int) -> HpcRun:
        try:
            response = self.client.get(url="/core/v1/simulator/status", params={"simulator_id": simulator_id})
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return HpcRun(**response.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not fetch build status for simulator {simulator_id}") from e

    def submit_get_simulator_status(self, simulator_id: int) -> str:
        try:
            status_update_response = self.client.get(
                url="/core/v1/simulator/status", params={"simulator_id": simulator_id}
            )
            if status_update_response.status_code != 200:
                raise httpx.HTTPError(  # noqa: TRY301
                    f"Server returned {status_update_response.status_code}: {status_update_response.text}"
                )
            return status_update_response.json().get("status", "")  # type: ignore[no-any-return]
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not fetch build status for simulator {simulator_id}: {e}") from e

    # -- Low-level HTTP methods: Simulation --

    def submit_run_workflow(
        self,
        params: httpx.QueryParams | None = None,
        experiment_id: str | None = None,
        simulator_id: int | None = None,
        config_filename: str | None = None,
        num_generations: int | None = None,
        num_seeds: int | None = None,
        description: str | None = None,
        run_parameter_calculator: bool | None = None,
        observables: list[str] | None = None,
        analysis_options: dict[str, object] | None = None,
        ecoli_sources_uri: str | None = None,
        ecoli_sources_overlays: str | None = None,
        ecoli_sources_repo_url: str | None = None,
        ecoli_sources_ref: str | None = None,
        tags: list[str] | None = None,
    ) -> Simulation:
        if params is not None:
            query_params = params
        else:
            # Build query items — httpx needs repeated keys for list params
            items: list[tuple[str, str | int | float | bool | None]] = [
                (k, str(v))
                for k, v in {
                    "simulator_id": simulator_id,
                    "simulation_config_filename": config_filename,
                    "num_generations": num_generations,
                    "num_seeds": num_seeds,
                    "description": description,
                    "ecoli_sources_uri": ecoli_sources_uri,
                    "ecoli_sources_overlays": ecoli_sources_overlays,
                    "ecoli_sources_repo_url": ecoli_sources_repo_url,
                    "ecoli_sources_ref": ecoli_sources_ref,
                    "experiment_id": experiment_id,
                    "run_parca": run_parameter_calculator,
                }.items()
                if v is not None
            ]
            if observables:
                items.extend(("observables", obs) for obs in observables)
            if tags:
                items.extend(("tags", tag) for tag in tags)
            query_params = httpx.QueryParams(items)
        try:
            json_body = analysis_options if analysis_options else None
            simulation_response = self.client.post(
                url="/api/v1/simulations",
                params=query_params,
                json=json_body,
            )
            if simulation_response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {simulation_response.status_code}: {simulation_response.text}")  # noqa: TRY301
            return Simulation(**simulation_response.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not submit a new simulation workflow with params {query_params}: {e}") from e

    def submit_get_workflow_status(self, simulation_id: int) -> SimulationRun:
        try:
            status_update_response = self.client.get(url=f"/api/v1/simulations/{simulation_id}/status")
            if status_update_response.status_code != 200:
                raise httpx.HTTPError(  # noqa: TRY301
                    f"Server returned {status_update_response.status_code}: {status_update_response.text}"
                )
            return SimulationRun(**status_update_response.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not load status for simulation {simulation_id}: {e}") from e

    def submit_cancel_workflow(self, simulation_id: int) -> SimulationRun:
        try:
            response = self.client.delete(url=f"/api/v1/simulations/{simulation_id}/cancel")
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return SimulationRun(**response.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not cancel simulation {simulation_id}") from e

    def submit_get_output_data(self, simulation_id: int) -> list[TsvOutputFile]:
        try:
            data_response = self.client.post(url=f"/api/v1/simulations/{simulation_id}/data")
            if data_response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {data_response.status_code}: {data_response.text}")  # noqa: TRY301
            return [TsvOutputFile(**output) for output in data_response.json()]
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not load output data for simulation {simulation_id}: {e}") from e

    def submit_get_workflow(self, simulation_id: int) -> Simulation:
        try:
            simulation = self.client.get(url=f"/api/v1/simulations/{simulation_id}")
            if simulation.status_code != 200:
                raise httpx.HTTPError(f"Server returned {simulation.status_code}: {simulation.text}")  # noqa: TRY301
            return Simulation(**simulation.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not load simulation {simulation_id}: {e}") from e

    def submit_add_tags(self, simulation_id: int, tags: list[str]) -> Simulation:
        try:
            response = self.client.post(url=f"/api/v1/simulations/{simulation_id}/tags", json={"tags": tags})
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return Simulation(**response.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not add tags to simulation {simulation_id}: {e}") from e

    def submit_list_workflows(self, experiment_id: str | None = None, tag: str | None = None) -> list[Simulation]:
        try:
            params: dict[str, str] = {}
            if experiment_id is not None:
                params["experiment_id"] = experiment_id
            if tag is not None:
                params["tag"] = tag
            simulations = self.client.get(url="/api/v1/simulations", params=params)
            if simulations.status_code != 200:
                raise httpx.HTTPError(f"Server returned {simulations.status_code}: {simulations.text}")  # noqa: TRY301
            return [Simulation(**sim) for sim in simulations.json()]
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not list simulations: {e}") from e

    def list_simulation_tags(self) -> dict[str, list[str]]:
        try:
            response = self.client.get(url="/api/v1/simulations/tags")
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            data: dict[str, list[str]] = response.json()
            return data
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not list simulation tags: {e}") from e

    def submit_get_workflow_log(self, simulation_id: int, truncate: bool = True) -> str:
        try:
            structured_log = self.client.get(
                url=f"/api/v1/simulations/{simulation_id}/log",
                params={"truncate": str(truncate).lower()},
            )
            if structured_log.status_code != 200:
                raise httpx.HTTPError(f"Server returned {structured_log.status_code}: {structured_log.text}")  # noqa: TRY301
            return structured_log.text
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not load log for simulation {simulation_id}: {e}") from e

    # -- Low-level HTTP methods: Parca --

    def submit_get_parca_datasets(self) -> list[ParcaDataset]:
        try:
            response = self.client.get(url="/core/v1/simulation/parca/versions")
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return [ParcaDataset(**ds) for ds in response.json()]
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError("Could not load parca datasets") from e

    def submit_get_parca_status(self, parca_id: int) -> HpcRun:
        try:
            response = self.client.get(url="/core/v1/simulation/parca/status", params={"parca_id": parca_id})
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return HpcRun(**response.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not load parca status for id {parca_id}") from e

    # -- Low-level HTTP methods: Analysis --

    def submit_get_analysis(self, analysis_id: int) -> ExperimentAnalysisDTO:
        try:
            response = self.client.get(url=f"/api/v1/analyses/{analysis_id}")
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return ExperimentAnalysisDTO(**response.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not load analysis {analysis_id}") from e

    def submit_get_analysis_status(self, analysis_id: int) -> AnalysisRun:
        try:
            response = self.client.get(url=f"/api/v1/analyses/{analysis_id}/status")
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return AnalysisRun(**response.json())
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not load analysis status for id {analysis_id}") from e

    def submit_get_analysis_log(self, analysis_id: int) -> str:
        try:
            response = self.client.get(url=f"/api/v1/analyses/{analysis_id}/log")
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return response.text
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not load analysis log for id {analysis_id}") from e

    def submit_get_analysis_plots(self, analysis_id: int) -> list[OutputFile]:
        try:
            response = self.client.get(url=f"/api/v1/analyses/{analysis_id}/plots")
            if response.status_code != 200:
                raise httpx.HTTPError(f"Server returned {response.status_code}: {response.text}")  # noqa: TRY301
            return [OutputFile(**p) for p in response.json()]
        except httpx.HTTPError:
            raise
        except Exception as e:
            raise httpx.HTTPError(f"Could not load analysis plots for id {analysis_id}") from e

    # -- Streaming output download --

    async def submit_stream_output_data(  # noqa: C901
        self, simulation_id: int, show_progress: bool = True, output_dirpath: Path | None = None, timeout: int = 1800
    ) -> set[str] | Path:
        """
        Download simulation output data as a streamable tar.gz archive.

        Args:
            simulation_id: The ID of the simulation to download data for.
            show_progress: If True, display a tqdm progress bar during download.
            output_dirpath: If provided, stream directly to a file
            found at: <OUTPUT_PATH>/<EXPERIMENT_ID>.tar.gz (memory-efficient
                for large archives). If None, keep in memory and return file basenames.

        Returns:
            If output_path is None: Set of archived file basenames.
            If output_path is provided: Path to the downloaded file.
        """
        async with async_client(base_url=self.base_url, timeout=timeout) as client:
            spinner_task = None
            if show_progress:
                # Start spinner while waiting for server to prepare data
                spinner_task = asyncio.create_task(
                    self._show_spinner(f"Waiting for server to gather simulation {simulation_id} files from HPC")
                )

            try:
                # Use stream=True to test actual streaming behavior
                async with client.stream("POST", f"/api/v1/simulations/{simulation_id}/data") as response:
                    # Stop spinner once we get a response
                    if spinner_task:
                        spinner_task.cancel()
                        try:  # noqa: SIM105
                            await spinner_task
                        except asyncio.CancelledError:
                            pass
                        # Clear the spinner line
                        sys.stdout.write("\r" + " " * 80 + "\r")
                        sys.stdout.flush()

                    if response.status_code != 200:
                        body = await response.aread()
                        raise httpx.HTTPError(message=body.decode(errors="replace"))  # noqa: TRY301

                    # Validate headers
                    if response.headers["content-type"] != "application/gzip":
                        raise httpx.HTTPError(  # noqa: TRY301
                            f"Unexpected MIME type for archive. Expected: {'application/gzip'}; "
                            f"Got: {response.headers['content-type']}"
                        )

                    content_disposition = response.headers.get("content-disposition", "")
                    if "attachment" not in content_disposition:
                        raise httpx.HTTPError(  # noqa: TRY301
                            f"Unexpected content-disposition header. Expected: {'attachment'}; "
                            f"Got: {content_disposition}"
                        )

                    # Get total size if available for progress bar
                    total_size = response.headers.get("content-length")
                    total_bytes = int(total_size) if total_size else None

                    if output_dirpath is not None:
                        # Stream directly to disk (memory-efficient for large files).
                        #
                        # Parse the output filename from the Content-Disposition header the
                        # server already set (e.g. ``attachment; filename="sim-foo.tar.gz"``).
                        # Previously this code made a *second* synchronous HTTP call to
                        # ``GET /api/v1/simulations/{id}`` just to rebuild the filename from
                        # the simulation's experiment_id — but opening a second connection to
                        # the same base_url while an async streaming response is still open
                        # can get RST'd over ``kubectl port-forward`` (HTTP/2 multiplex
                        # weirdness), which would abort an otherwise-successful download.
                        archive_filename = _parse_content_disposition_filename(content_disposition)
                        if archive_filename is None:
                            raise httpx.HTTPError(  # noqa: TRY301
                                f"Could not parse filename from content-disposition header: {content_disposition}"
                            )
                        output_path = output_dirpath / archive_filename
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(output_path, "wb") as f:
                            if show_progress:
                                with tqdm(
                                    total=total_bytes,
                                    unit="B",
                                    unit_scale=True,
                                    unit_divisor=1024,
                                    desc=f"Downloading to {output_path.name}",
                                    dynamic_ncols=True,
                                ) as pbar:
                                    async for chunk in response.aiter_bytes():
                                        f.write(chunk)
                                        pbar.update(len(chunk))
                            else:
                                async for chunk in response.aiter_bytes():
                                    f.write(chunk)
                        return output_path
                    else:
                        # Collect in memory (original behavior)
                        chunks = []
                        if show_progress:
                            with tqdm(
                                total=total_bytes,
                                unit="B",
                                unit_scale=True,
                                unit_divisor=1024,
                                desc="Downloading",
                                dynamic_ncols=True,
                            ) as pbar:
                                async for chunk in response.aiter_bytes():
                                    chunks.append(chunk)
                                    pbar.update(len(chunk))
                        else:
                            async for chunk in response.aiter_bytes():
                                chunks.append(chunk)

                        # Verify we actually got data
                        if len(chunks) < 1:
                            raise httpx.HTTPError("No data was streamed.")  # noqa: TRY301
                        content = b"".join(chunks)

            except Exception:
                if spinner_task and not spinner_task.done():
                    spinner_task.cancel()
                raise

        # If we got here, output_path was None - process in-memory content
        # Validate it's valid gzip
        decompressed = gzip.decompress(content)

        # Validate it's a valid tar archive with expected structure
        tar_buffer = io.BytesIO(decompressed)
        with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
            archived_names = set(tar.getnames())
            # Extract basenames from archived files for comparison
            archived_basenames = {Path(name).name for name in archived_names}

        return archived_basenames

    @staticmethod
    async def _show_spinner(message: str) -> None:
        """Display a spinner animation while waiting."""
        spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        idx = 0
        while True:
            sys.stdout.write(f"\r{spinner_chars[idx]} {message}...")
            sys.stdout.flush()
            idx = (idx + 1) % len(spinner_chars)
            await asyncio.sleep(0.1)

    # -- Compose (process-bigraph) --

    def compose_list_simulators(self) -> dict:  # type: ignore[type-arg]
        resp = self.client.get("/compose/v1/simulators")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_list_processes(self) -> list[dict]:  # type: ignore[type-arg]
        resp = self.client.get("/compose/v1/processes")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_list_steps(self) -> list[dict]:  # type: ignore[type-arg]
        resp = self.client.get("/compose/v1/steps")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_run_simulation(self, file_path: Path, interval_time: float = 1.0, batch: bool = False) -> dict:  # type: ignore[type-arg]
        with open(file_path, "rb") as f:
            resp = self.client.post(
                "/compose/v1/simulation/run",
                files={"uploaded_file": (file_path.name, f)},
                params={"interval_time": interval_time, "batch_submission": batch},
            )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_get_simulations_status_batch(self, simulation_ids: list[int]) -> list:  # type: ignore[type-arg]
        """Status for MANY compose simulations in one call.

        ``GET /compose/v1/simulations/status/batch?ids=1&ids=2`` — repeated
        ``ids`` params, so ``params`` takes a list and httpx expands it. The
        single-id endpoint beside this one is fine for one run; a campaign is
        where it stops being fine, which is why viva-api grew this and why the
        workbench's job layer polls through it rather than looping.
        """
        resp = self.client.get("/compose/v1/simulations/status/batch", params={"ids": simulation_ids})
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    # -- env-worker relay (plan §C) -----------------------------------------
    #
    # viva-api holds the worker socket and forwards JSON-RPC over HTTP, which is
    # what makes these reachable from a LAPTOP at all: a worker dials back, and
    # an SSM tunnel is laptop-initiated with no inbound path, so the workbench's
    # own dial-back transport has no address to advertise from here.
    #
    # Plain httpx like the compose methods above, rather than the generated
    # client: these are three small calls, and the generated wrappers would add
    # a model-conversion layer for payloads that are already the shape the CLI
    # prints.

    def worker_start(
        self,
        commit: str,
        workspace: str | None = None,
        session_key: str | None = None,
        accept_timeout: float | None = None,
    ) -> dict:  # type: ignore[type-arg]
        """Start a relayed env worker and wait for it to dial back.

        The call returns only once the worker has connected (or the server gives
        up), because a handle to a worker that never arrived is not useful --
        ``accept_timeout`` covers pod scheduling and image pull, so it is
        generous and separate from any per-call timeout.
        """
        body: dict[str, object] = {"commit": commit}
        if workspace:
            body["workspace"] = workspace
        if session_key:
            body["session_key"] = session_key
        if accept_timeout is not None:
            body["accept_timeout"] = accept_timeout
        # Outlive the server's own accept window rather than racing it: timing
        # out here would abandon a worker the server is still holding open for.
        client_timeout = (accept_timeout or 300.0) + 60.0
        resp = self.client.post("/env-worker/v1/relay/workers", json=body, timeout=client_timeout)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def worker_call(
        self,
        job_name: str,
        method: str,
        params: dict | None = None,  # type: ignore[type-arg]
        timeout: float = 300.0,
    ) -> dict:  # type: ignore[type-arg]
        """Forward one JSON-RPC call to a relayed worker."""
        body = {"method": method, "params": params or {}, "timeout": timeout}
        resp = self.client.post(f"/env-worker/v1/relay/workers/{job_name}/call", json=body, timeout=timeout + 60.0)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def worker_stop(self, job_name: str) -> dict:  # type: ignore[type-arg]
        """Drop the connection and delete the Job. Idempotent server-side."""
        resp = self.client.delete(f"/env-worker/v1/relay/workers/{job_name}")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_get_simulation_status(self, simulation_id: int) -> dict:  # type: ignore[type-arg]
        resp = self.client.get(f"/compose/v1/simulation/{simulation_id}/status")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_get_simulation_results(self, simulation_id: int, dest: Path) -> Path:
        resp = self.client.get(f"/compose/v1/simulation/{simulation_id}/results")
        resp.raise_for_status()
        dest.mkdir(parents=True, exist_ok=True)
        out_file = dest / f"compose_results_{simulation_id}.zip"
        out_file.write_bytes(resp.content)
        return out_file

    def compose_get_simulation_document(self, simulation_id: int) -> dict:  # type: ignore[type-arg]
        resp = self.client.get(f"/compose/v1/simulation/{simulation_id}/document")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_get_build_status(self, simulator_id: int) -> dict:  # type: ignore[type-arg]
        resp = self.client.get(f"/compose/v1/simulator/{simulator_id}/build/status")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_run_v2ecoli(
        self,
        duration: float = 60.0,
        seed: int = 0,
        interval: float = 1.0,
        features: str = "[]",
        cache_dir: str = "out/cache",
    ) -> dict:  # type: ignore[type-arg]
        resp = self.client.post(
            "/compose/v1/curated/ecoli",
            params={
                "duration": duration,
                "seed": seed,
                "interval": interval,
                "features": features,
                "cache_dir": cache_dir,
            },
        )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_run_copasi(self, sbml_path: Path, start_time: float, duration: float, num_data_points: float) -> dict:  # type: ignore[type-arg]
        with open(sbml_path, "rb") as f:
            resp = self.client.post(
                "/compose/v1/curated/copasi",
                files={"sbml": (sbml_path.name, f)},
                params={"start_time": start_time, "duration": duration, "num_data_points": num_data_points},
            )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_biomodels_identifiers(self, n: int = 20) -> list[str]:
        resp = self.client.get("/compose/v1/biomodels/identifiers", params={"n": n})
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_biomodels_metadata(self, biomodel_id: str) -> dict:  # type: ignore[type-arg]
        resp = self.client.get(f"/compose/v1/biomodels/{biomodel_id}/metadata")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_biomodels_run(self, biomodel_id: str, simulator: str = "copasi") -> dict:  # type: ignore[type-arg]
        resp = self.client.post(
            f"/compose/v1/biomodels/{biomodel_id}/run",
            params={"simulator": simulator},
        )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_biomodels_batch(
        self,
        simulator: str = "copasi",
        model_ids: list[str] | None = None,
        n_models: int | None = None,
    ) -> dict:  # type: ignore[type-arg]
        payload: dict[str, object] = {"simulator": simulator}
        if model_ids is not None:
            payload["model_ids"] = model_ids
        if n_models is not None:
            payload["n_models"] = n_models
        resp = self.client.post("/compose/v1/biomodels/batch", json=payload)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_biomodels_audit(
        self,
        biomodel_id: str,
        simulators: list[str] | None = None,
    ) -> dict:  # type: ignore[type-arg]
        params: dict[str, list[str]] = {}
        if simulators is not None:
            params["simulators"] = simulators
        resp = self.client.post(f"/compose/v1/biomodels/{biomodel_id}/audit", params=params)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_biomodels_regression(
        self,
        n_models: int = 10,
        model_ids: list[str] | None = None,
        simulators: list[str] | None = None,
    ) -> dict:  # type: ignore[type-arg]
        payload: dict[str, object] = {"n_models": n_models}
        if model_ids is not None:
            payload["model_ids"] = model_ids
        if simulators is not None:
            payload["simulators"] = simulators
        resp = self.client.post("/compose/v1/biomodels/regression", json=payload)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def compose_run_tellurium(
        self, sbml_path: Path, start_time: float, end_time: float, num_data_points: float
    ) -> dict:  # type: ignore[type-arg]
        with open(sbml_path, "rb") as f:
            resp = self.client.post(
                "/compose/v1/curated/tellurium",
                files={"sbml": (sbml_path.name, f)},
                params={"start_time": start_time, "end_time": end_time, "num_data_points": num_data_points},
            )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]


def get_data_service(base_url: BaseUrl | str | None = None, timeout: int | None = None) -> E2EDataService:
    return E2EDataService(
        base_url=BaseUrl(base_url) if base_url else DEFAULT_BASE_URL, timeout=timeout or DEFAULT_REQUEST_TIMEOUT
    )
