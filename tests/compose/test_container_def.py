from viva_api.compose.container_def import ContainerizationFileRepr


def test_dto_holds_representation() -> None:
    r = ContainerizationFileRepr(representation="Bootstrap: docker")
    assert r.representation == "Bootstrap: docker"
