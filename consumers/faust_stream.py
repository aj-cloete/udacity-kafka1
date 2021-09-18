"""Defines trends calculations for stations"""
import logging

import faust
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("com.udacity.stations", value_type=Station)
out_topic = app.topic("com.udacity.transformed.stations", value_type=TransformedStation, partitions=1)
table = app.Table(
   "transformed_stations",
   default=str,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def station_transformation(stations):
    async for station in stations:
        table['station_id'] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=("red" if station.red else
                  "blue" if station.blue else
                  "green" if station.green else None
                  )
        )

if __name__ == "__main__":
    app.main()
