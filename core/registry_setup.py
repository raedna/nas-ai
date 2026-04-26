from core.orchestrator import ComponentRegistry
from XML.xml_parser import parse_xml_rows
from XML.xml_serializer import xml_serializer
from TABLES.table_parser import parse_table
from TABLES.table_serializer import table_serializer
from DOCS.doc_parser import parse_doc
from DOCS.doc_serializer import doc_serializer
from IMAGES.image_parser import parse_image
from IMAGES.image_serializer import serialize_image
from PDF.pdf_parser import parse_pdf
from PDF.pdf_serializer import pdf_serializer
from ASTRO.astro_parser import parse_astro
from ASTRO.astro_serializer import astro_serializer

registry = ComponentRegistry()

registry.register_parser("xml", parse_xml_rows)
registry.register_serializer("xml", xml_serializer)

registry.register_parser("table", parse_table)
registry.register_serializer("table", table_serializer)

registry.register_parser("doc", parse_doc)
registry.register_serializer("doc", doc_serializer)

registry.register_parser("image", parse_image)
registry.register_serializer("image", serialize_image)

registry.register_parser("pdf", parse_pdf)
registry.register_serializer("pdf", pdf_serializer)

registry.register_parser("astro", parse_astro)
registry.register_serializer("astro", astro_serializer)