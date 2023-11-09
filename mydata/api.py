import glob
import os
import uuid
from datetime import datetime
from typing import Any, List, Optional

from rdflib import Graph, URIRef

from mydata.namespace import MY, OWNLD
from mydata.utils import get_file_hash


class DiscoverAndParse:
    glob_pattern: List[str] = []
    graph_uri: URIRef
    graph_types: List[URIRef] = []
    script_record: Optional[dict[str, Any]] = None

    def parse_file(self, file):
        raise NotImplementedError

    def run(self):
        graph = Graph()

        run_uuid = uuid.uuid1().hex[:8]
        start_time = datetime.now()

        # discover and parse files
        dependencies = []
        for pattern in self.glob_pattern:
            for file in glob.glob(pattern, recursive=True):
                print(f"Parsing {file}")
                file_hash = get_file_hash(file)

                records = self.parse_file(file)

                n_records = 0
                for record in records:
                    graph.parse(data=record, format="json-ld")
                    n_records += 1

                if n_records > 0:
                    file_dep = {
                        "@id": MY[f"file/{file_hash}"],
                        "@type": OWNLD["core#File"],
                        "hash": file_hash,
                        "absolutePath": os.path.abspath(file),
                        "size": os.path.getsize(file),
                        "createdAt": datetime.fromtimestamp(os.path.getctime(file)),
                        "modifiedAt": datetime.fromtimestamp(os.path.getmtime(file)),
                    }
                    dependencies.append(file_dep)

        # add graph and source metadata
        graph_metadata = {
            "@context": {"@vocab": OWNLD["core#"]},
            "@id": self.graph_uri,
            "@type": [OWNLD["core#Graph"]] + self.graph_types,
            "source": {
                "@type": OWNLD["core#Source"],
                "@id": MY[f"source/{start_time.isoformat()}/{run_uuid}"],
                "script": self.script_record,
                "dependsOn": dependencies,
                "createdAt": start_time,
                "parsedTriples": len(graph),
                "parsingTime": datetime.now() - start_time,
            },
        }
        graph.parse(data=graph_metadata, format="json-ld")

        return graph, self.graph_uri
