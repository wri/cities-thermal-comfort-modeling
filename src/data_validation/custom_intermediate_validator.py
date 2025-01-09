

def evaluate_custom_intermediate_config(non_tiled_city_data):
    custom_intermediate_features = non_tiled_city_data.custom_intermediate_list

    invalids = []

    if not custom_intermediate_features:
        return invalids

    # wall dependencies
    if not ('wallaspect' in custom_intermediate_features and 'wallheight' in custom_intermediate_features):
        msg = 'Both wall_aspect_filename and wall_height_filename must both be specified if one of them is specified as not None.'
        invalids.append((msg, True))
    else:
        if 'dsm' in non_tiled_city_data.cif_primary_feature_list:
            msg = 'dsm_tif_filename cannot be None if wallaspect and wallheight are not None due, to dependency conflict.'
            invalids.append((msg, True))

    if 'skyview_factor' in custom_intermediate_features and 'dsm' in non_tiled_city_data.cif_primary_feature_list:
        msg = 'dsm_tif_filename cannot be None if skyview_factor_filename is not None, due to dependency conflict.'
        invalids.append((msg, True))

    if 'skyview_factor' in custom_intermediate_features and 'tree_canopy' in non_tiled_city_data.cif_primary_feature_list:
        msg = 'tree_canopy_tif_filename cannot be None if skyview_factor_filename is not None, due to dependency conflict.'
        invalids.append((msg, True))

    return invalids
