import json
import pathlib

import pandas as pd
import requests
import xmltodict

from viva_api.config import Settings, get_settings
from viva_api.data.models import BiocycData


class BiocycService:
    settings: Settings
    session: requests.Session

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self.session = login_biocyc(biocyc_email=self.settings.biocyc_email, pw=self.settings.biocyc_password)

    def __del__(self) -> None:
        self.session.close()

    def get_data(self, obj_id: str, org_id: str | None = None) -> BiocycData:
        return get_biocyc_data(session=self.session, objid=obj_id, orgid=org_id or "ECOLI")

    def write_batch(self, obj_ids: list[str]) -> None:
        return write_biocyc_batch(session=self.session, objids=obj_ids)

    def load_tsv(self, csv_filename: str) -> pd.DataFrame:
        return load_flat(csv_filename)


def login_biocyc(biocyc_email: str, pw: str) -> requests.Session:
    s = requests.Session()
    try:
        creds = {"email": biocyc_email, "password": pw}
        resp = s.post("https://websvc.biocyc.org/credentials/login/", data=creds)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise requests.exceptions.HTTPError(f"Biocyc login failed: {e}")
    return s


def get_biocyc_data(session: requests.Session, orgid: str, objid: str) -> BiocycData:
    url = f"https://websvc.biocyc.org/getxml?id={orgid.upper()}:{objid}&detail=low&fmt=json"
    r = session.get(url, headers={"Accept": "application/json"})
    r.raise_for_status()
    xml = r.text
    data_dict = xmltodict.parse(xml)
    json_data = json.loads(json.dumps(data_dict, indent=2))
    request_data = {"url": url, "headers": {"Accept": "application/json"}}

    return BiocycData(obj_id=objid, org_id=orgid, data=json_data, request=request_data)


def write_biocyc_batch(session: requests.Session, objids: list[str]) -> None:
    dest_fp = pathlib.Path("assets/biocyc")
    for m_id in objids:
        orgid = "ECOLI"
        m_fp = dest_fp / f"{m_id}.json"
        if not m_fp.exists():
            data_i = get_biocyc_data(session=session, objid=m_id, orgid=orgid)
            data_i.export()
        else:
            print(f"There is already data for: {m_id} at {m_fp}")
    return None


def load_flat(csv_filename: str) -> pd.DataFrame:
    return pd.read_csv(
        pathlib.Path(f"/Users/alexanderpatrie/sms/vEcoli/reconstruction/ecoli/flat/{csv_filename}.tsv"),
        sep="\t",
        comment="#",
    )


def get_metabolite_ids() -> list[str]:
    met = load_flat("metabolites")
    return met[["id"]].to_numpy().flatten().tolist()
