from dataclasses import dataclass
from dataclasses import field
from importlib.resources import files
from typing import Generator
from typing import Iterator
from typing import List
from typing import Literal
from typing import LiteralString
from typing import Optional

import yaml

from odapi.resources.ckan.ckan import CkanResource


@dataclass
class ConfigSourceLoad:
    delimiter: str = ','


@dataclass
class ConfigSource:
    name: str
    type: Literal['OpenDataSwiss', 'CustomAsset']
    ckan_resource_id: Optional[str] = None
    load: ConfigSourceLoad = field(default_factory=ConfigSourceLoad)

    def model_name(self, parent_name: str) -> str:
        return '_'.join([parent_name, self.name])


@dataclass
class ConfigModel:
    name: str
    publisher_type: Literal['MultiplePublisher', 'SinglePublisher']
    description: str
    sources: List[ConfigSource]

    @property
    def model_name(self) -> str:
        return f'mart_{self.name}'


class PublishedModels:
    PATH = files('odapi').joinpath('published_models.yml')

    def __init__(self):
        with self.PATH.open('r', encoding='utf-8') as file:
            self.data = yaml.safe_load(file)

    @property
    def _models_dict(self) -> List[dict]:
        return self.data['models']

    @property
    def model_names(self) -> List[str]:
        return [model['name'] for model in self._models_dict]

    def _model_dict_by_id(self, model: str) -> dict:
        return [mo for mo in self._models_dict if mo['name'] == model][0]

    @property
    def models(self) -> List[ConfigModel]:
        col_config = []
        for model in self.model_names:
            _model_data = self._model_dict_by_id(model)
            col_sources = self.sources_by_model(model)
            col_config.append(
                ConfigModel(
                    name=model,
                    publisher_type=_model_data.get('publisher_type', ''),
                    description=_model_data.get('description', ''),
                    sources=col_sources,
                )
            )
        return col_config

    def sources_by_model(self, model: str) -> List[ConfigSource]:
        sources = []
        for source in self._model_dict_by_id(model)['sources']:
            if c := source.get('load', None):
                load_config = ConfigSourceLoad(**c)
                source['load'] = load_config
            sources.append(ConfigSource(**source))
        return sources

    @property
    def ckan_resources(self) -> Iterator[CkanResource]:
        for model in self.model_names:
            for source in self.sources_by_model(model):
                if source.type == 'OpenDataSwiss':
                    if source.ckan_resource_id is None:
                        raise ValueError(
                            f'Model {model}, Source {source.name}: ckan_resource_id '
                            f'must be set for sources of type OpenDataSwiss.'
                        )
                    yield CkanResource(
                        model_name=source.model_name(model),
                        ckan_resource_id=source.ckan_resource_id,
                        delimiter=source.load.delimiter,
                    )

    @property
    def model_resources(self) -> List[dict]:
        data = []
        for model in self.models:
            model_data = {}
            model_data['name'] = model.model_name
            model_data['publisher_type'] = model.publisher_type
            model_data['description'] = model.description
            data.append(model_data)
        return data
