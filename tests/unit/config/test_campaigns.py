from __future__ import annotations

from pathlib import Path

import pytest

from rundra.config.campaigns import load_campaign
from rundra.config.errors import ConfigError
from rundra.config.launch import load_project_launch
from rundra.domain.campaigns import CampaignFailurePolicy


def test_standalone_campaign_resolves_launches_and_paths(tmp_path: Path) -> None:
    source = tmp_path / "campaign.yaml"
    source.write_text(
        """\
kind: campaign
version: 1
name: dual
experiment: experiment.yaml
on_submit_failure: stop
launches:
  - name: shoal
    target: shoal
    seeds: 0:31
    destination: results/shoal
  - name: isir
    profile: isir-cpu
    seed: 32
""",
        encoding="utf-8",
    )

    campaign = load_campaign(source)

    assert campaign.experiment == (tmp_path / "experiment.yaml").resolve()
    assert campaign.on_submit_failure is CampaignFailurePolicy.STOP
    assert campaign.launches[0].seeds.count == 32
    assert campaign.launches[0].destination == (tmp_path / "results/shoal").resolve()
    assert campaign.launches[1].seeds.start == 32


def test_project_v7_accepts_named_campaign_without_preparation(tmp_path: Path) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(
        """\
version: 7
profiles:
  shoal-cpu: {target: shoal}
campaigns:
  dual:
    launches:
      - {name: shoal, profile: shoal-cpu, seeds: '0:3'}
""",
        encoding="utf-8",
    )

    project = load_project_launch(source)

    assert project.version == 7
    assert project.campaigns["dual"].launches[0].profile == "shoal-cpu"


@pytest.mark.parametrize(
    "content,code",
    [
        (
            "kind: campaign\nversion: 1\nname: x\nexperiment: e.yaml\nlaunches: []\n",
            "INVALID_TYPE",
        ),
        (
            "kind: campaign\nversion: 1\nname: x\nexperiment: e.yaml\nlaunches: [{name: A, seed: 1}]\n",
            "INVALID_VALUE",
        ),
        (
            "kind: campaign\nversion: 1\nname: x\nexperiment: e.yaml\nlaunches: [{name: a, seed: 1, token: no}]\n",
            "FORBIDDEN_FIELD",
        ),
    ],
)
def test_campaign_rejects_invalid_documents(
    tmp_path: Path, content: str, code: str
) -> None:
    source = tmp_path / "campaign.yaml"
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_campaign(source)

    assert caught.value.code == code
