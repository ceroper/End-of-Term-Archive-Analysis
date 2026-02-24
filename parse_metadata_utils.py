import pandas as pd
from warcio.archiveiterator import ArchiveIterator

def get_metadata(record):
    # Collect all user-defined attributes (excluding special methods and dunder methods)
    all_attrs = dir(record)
    # Remove '.__class__', '__delattr__', etc.
    metadata = {attr: getattr(record, attr) for attr in all_attrs 
                if not attr.startswith("__")}
    try:
        content = record.content_stream().read()
        metadata['content'] = content[:1000]
    except Exception as e:
        pass
    return metadata

def extract_outlinks(metadata_bytes):
    """
    Extract all outlink URLs from a WARC metadata record's bytes content.
    """
    # Step 1: Decode bytes to string (assuming UTF-8)
    try:
        meta_str = metadata_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        print("Decoding failed:", e)
        return None

    # Step 2: Split into lines
    lines = meta_str.splitlines()

    # Step 3: Collect all outlinks
    outlinks = []
    for line in lines:
        if line.startswith('outlink: '):
            # Strip whitespace and extract the URL after ':'
            url_info = line.split(':', 1)[1].strip() if ':' in line else ''
            url = url_info.split(' ')[0]
            type = ' '.join(url_info.split(' ')[1:])
            outlinks.append((url, type))
    # Optionally, deduplicate
    # outlinks = list(dict.fromkeys(outlinks))  # Remove duplicates while preserving order (Python 3.7+)
    return outlinks