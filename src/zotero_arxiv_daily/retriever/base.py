from abc import ABC, abstractmethod
from omegaconf import DictConfig
from ..protocol import Paper, RawPaperItem
from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError
from typing import Type
from loguru import logger
class BaseRetriever(ABC):
    name: str
    def __init__(self, config:DictConfig):
        self.config = config
        self.retriever_config = getattr(config.source,self.name)

    @abstractmethod
    def _retrieve_raw_papers(self) -> list[RawPaperItem]:
        pass

    @abstractmethod
    def convert_to_paper(self, raw_paper:RawPaperItem) -> Paper | None:
        pass

    def retrieve_papers(self) -> list[Paper]:
        raw_papers = self._retrieve_raw_papers()
        papers = []
        logger.info("Processing papers...")
        exec_pool = ProcessPoolExecutor(max_workers=self.config.executor.max_workers)
        futures = {exec_pool.submit(self.convert_to_paper, rp): rp for rp in raw_papers}
        for future in as_completed(futures, timeout=len(raw_papers) * 120):
            try:
                paper = future.result(timeout=10)
                if paper is not None:
                    papers.append(paper)
            except TimeoutError:
                logger.warning("Timeout processing paper, skipping")
            except Exception as e:
                logger.warning(f"Error processing paper: {e}")
        exec_pool.shutdown(wait=False, cancel_futures=True)
        return papers

registered_retrievers = {}

def register_retriever(name:str):
    def decorator(cls):
        registered_retrievers[name] = cls
        cls.name = name
        return cls
    return decorator

def get_retriever_cls(name:str) -> Type[BaseRetriever]:
    if name not in registered_retrievers:
        raise ValueError(f"Retriever {name} not found")
    return registered_retrievers[name]