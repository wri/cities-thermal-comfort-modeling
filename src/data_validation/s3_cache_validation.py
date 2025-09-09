from city_metrix import TreeCanopyHeight
from city_metrix.constants import CTCM_PADDED_AOI_BUFFER, GeoType
from city_metrix.layers import FabDEM, OpenUrban, OvertureBuildingsDSM, AlbedoCloudMasked
from city_metrix.cache_manager import determine_cache_usability
from src.constants import S3_PUBLICATION_BUCKET, S3_PUBLICATION_ENV

def check_cache_availability(city_geoextent):
    invalids = []

    if city_geoextent.geo_type == GeoType.CITY:
        # FabDEM
        invalid = _check_layer_availability(FabDEM(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

        # OvertureBuildingsDSM
        invalid = _check_layer_availability(OvertureBuildingsDSM(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

        # OpenUrban
        invalid = _check_layer_availability(OpenUrban(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

        # TreeCanopyHeight
        invalid = _check_layer_availability(TreeCanopyHeight(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

        # AlbedoCloudMasked
        invalid = _check_layer_availability(AlbedoCloudMasked(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

    return invalids

def _check_layer_availability(layer_object, city_geoextent):
    layer_has_usable_cache = determine_cache_usability(S3_PUBLICATION_BUCKET, S3_PUBLICATION_ENV, layer_object, city_geoextent,
                                                     aoi_buffer_m=CTCM_PADDED_AOI_BUFFER)
    if not layer_has_usable_cache:
        msg = (f"Cache not found for {city_geoextent.city_id}, {city_geoextent.aoi_id}, {layer_object.__class__.__name__}, "
               f"buffer:{CTCM_PADDED_AOI_BUFFER} in {S3_PUBLICATION_BUCKET} {S3_PUBLICATION_ENV}.")
        return (msg, True)
    else:
        return None
