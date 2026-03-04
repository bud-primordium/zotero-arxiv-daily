from .base import BaseRetriever, register_retriever
import arxiv
from arxiv import Result as ArxivResult
from ..protocol import Paper
from ..utils import extract_markdown_from_pdf
from tempfile import TemporaryDirectory
import feedparser
from urllib.request import urlopen
from tqdm import tqdm
import os
import time
import shutil
from loguru import logger

_DOWNLOAD_TIMEOUT = 120  # total seconds for downloading one PDF
_SOCKET_TIMEOUT = 30     # per-socket-operation timeout

@register_retriever("arxiv")
class ArxivRetriever(BaseRetriever):
    def __init__(self, config):
        super().__init__(config)
        if self.config.source.arxiv.category is None:
            raise ValueError("category must be specified for arxiv.")
    def _retrieve_raw_papers(self) -> list[ArxivResult]:
        client = arxiv.Client(num_retries=10,delay_seconds=10)
        query = '+'.join(self.config.source.arxiv.category)
        # Get the latest paper from arxiv rss feed
        feed = feedparser.parse(f"https://rss.arxiv.org/atom/{query}")
        if 'Feed error for query' in feed.feed.title:
            raise Exception(f"Invalid ARXIV_QUERY: {query}.")
        raw_papers = []
        all_paper_ids = [i.id.removeprefix("oai:arXiv.org:") for i in feed.entries if i.get("arxiv_announce_type","new") == 'new']
        if self.config.executor.debug:
            all_paper_ids = all_paper_ids[:10]

        # Get full information of each paper from arxiv api
        bar = tqdm(total=len(all_paper_ids))
        for i in range(0,len(all_paper_ids),20):
            search = arxiv.Search(id_list=all_paper_ids[i:i+20])
            batch = list(client.results(search))
            bar.update(len(batch))
            raw_papers.extend(batch)
        bar.close()

        return raw_papers

    @staticmethod
    def _download_with_timeout(url: str, path: str):
        """Download file with both per-socket and total timeout."""
        start = time.monotonic()
        with urlopen(url, timeout=_SOCKET_TIMEOUT) as resp:
            with open(path, 'wb') as f:
                while True:
                    if time.monotonic() - start > _DOWNLOAD_TIMEOUT:
                        raise TimeoutError(f"Download exceeded {_DOWNLOAD_TIMEOUT}s")
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)

    def convert_to_paper(self, raw_paper:ArxivResult) -> Paper:
        title = raw_paper.title
        authors = [a.name for a in raw_paper.authors]
        abstract = raw_paper.summary
        pdf_url = raw_paper.pdf_url
        full_text = None
        try:
            with TemporaryDirectory() as temp_dir:
                path = os.path.join(temp_dir, "paper.pdf")
                self._download_with_timeout(pdf_url, path)
                full_text = extract_markdown_from_pdf(path)
        except Exception as e:
            logger.warning(f"Failed to extract full text of {title}: {e}")
        return Paper(
            source=self.name,
            title=title,
            authors=authors,
            abstract=abstract,
            url=raw_paper.entry_id,
            pdf_url=pdf_url,
            full_text=full_text
        )
