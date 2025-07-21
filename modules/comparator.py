def compare_hashes(source_hashes, target_hashes):
    """
    Compares two dicts of {pk: hash}. Returns:
    - mismatches: PKs where hashes differ
    - missing_in_source: PKs present in target but not in source
    - missing_in_target: PKs present in source but not in target
    """
    mismatches = []
    missing_in_source = []
    missing_in_target = []
    for pk, src_hash in source_hashes.items():
        tgt_hash = target_hashes.get(pk)
        if tgt_hash is None:
            missing_in_target.append(pk)
        elif src_hash != tgt_hash:
            mismatches.append(pk)
    for pk in target_hashes:
        if pk not in source_hashes:
            missing_in_source.append(pk)
    return mismatches, missing_in_source, missing_in_target 