import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Album
Album_node1707996678787 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://project-spotify-de/staging/albums.csv"],
        "recurse": True,
    },
    transformation_ctx="Album_node1707996678787",
)

# Script generated for node Track
Track_node1707996679348 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://project-spotify-de/staging/track.csv"],
        "recurse": True,
    },
    transformation_ctx="Track_node1707996679348",
)

# Script generated for node Artist
Artist_node1707996677923 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://project-spotify-de/staging/artists.csv"],
        "recurse": True,
    },
    transformation_ctx="Artist_node1707996677923",
)

# Script generated for node Join Artist & Album
JoinArtistAlbum_node1707996801513 = Join.apply(
    frame1=Artist_node1707996677923,
    frame2=Album_node1707996678787,
    keys1=["id"],
    keys2=["artist_id"],
    transformation_ctx="JoinArtistAlbum_node1707996801513",
)

# Script generated for node Join with Track
JoinwithTrack_node1707996907879 = Join.apply(
    frame1=Track_node1707996679348,
    frame2=JoinArtistAlbum_node1707996801513,
    keys1=["track_id"],
    keys2=["track_id"],
    transformation_ctx="JoinwithTrack_node1707996907879",
)

# Script generated for node Drop Fields
DropFields_node1707996980432 = DropFields.apply(
    frame=JoinwithTrack_node1707996907879,
    paths=["id", "`.track_id`"],
    transformation_ctx="DropFields_node1707996980432",
)

# Script generated for node Destination
Destination_node1707997028439 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1707996980432,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://project-spotify-de/datawarehouse/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Destination_node1707997028439",
)

job.commit()
