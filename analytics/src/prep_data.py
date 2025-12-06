import polars as pl


def get_player_data():
    return (
        pl.scan_csv("data/train/input*.csv")
        .select(
            "nfl_id",
            "player_name",
            "player_height",
            "player_weight",
            "player_position",
            "player_side",
        )
        .unique()
        .collect()
    )


def get_supplementary_data():
    return pl.read_csv(
        "data/supplementary_data.csv", null_values=["NA", "N/A", ""]
    ).with_columns(
        distance_to_goal=pl.when(pl.col("yardline_side") == pl.col("possession_team"))
        .then(100 - pl.col("yardline_number"))
        .otherwise(pl.col("yardline_number"))
    )


def get_tracking_output():
    return pl.read_csv(
        "data/train/output_*.csv", null_values=["NA", "nan", "N/A", "NaN", ""]
    )


def get_tracking_input():
    return pl.read_csv(
        "data/train/input_*.csv",
        quote_char='"',
        null_values=["NA", "nan", "N/A", "NaN", ""],
    ).join(
        get_supplementary_data().select(["game_id", "play_id", "distance_to_goal"]),
        on=["game_id", "play_id"],
    )


def convert_tracking_to_cartesian(tracking_df):
    return (
        tracking_df.with_columns(
            dir=((pl.col("dir") - 90) * -1) % 360,
            o=((pl.col("o") - 90) * -1) % 360,
        )
        # convert polar vectors to cartesian ((s, dir) -> (vx, vy), (o) -> (ox, oy))
        .with_columns(
            vx=pl.col("s") * pl.col("dir").radians().cos(),
            vy=pl.col("s") * pl.col("dir").radians().sin(),
            ox=pl.col("o").radians().cos(),
            oy=pl.col("o").radians().sin(),
        )
    )


def standardize_tracking_directions(tracking_df):
    df = tracking_df.with_columns(
        x=pl.when(pl.col("play_direction") == "right")
        .then(pl.col("x"))
        .otherwise(110 - pl.col("x")),
        y=pl.when(pl.col("play_direction") == "right")
        .then(pl.col("y"))
        .otherwise(53.3 - pl.col("y")),
    ).with_columns(x=pl.col("x") - (110 - pl.col("distance_to_goal")))

    if "ball_land_x" in df.columns:
        df = convert_tracking_to_cartesian(df)
        df = df.with_columns(
            ball_land_x=pl.when(pl.col("play_direction") == "right")
            .then(pl.col("ball_land_x"))
            .otherwise(110 - pl.col("ball_land_x")),
            ball_land_y=pl.when(pl.col("play_direction") == "right")
            .then(pl.col("ball_land_y"))
            .otherwise(53.3 - pl.col("ball_land_y")),
            vx=pl.when(pl.col("play_direction") == "right")
            .then(pl.col("vx"))
            .otherwise(pl.col("vx") * -1),
            vy=pl.when(pl.col("play_direction") == "right")
            .then(pl.col("vy"))
            .otherwise(pl.col("vy") * -1),
            ox=pl.when(pl.col("play_direction") == "right")
            .then(pl.col("ox"))
            .otherwise(pl.col("ox") * -1),
            oy=pl.when(pl.col("play_direction") == "right")
            .then(pl.col("oy"))
            .otherwise(pl.col("oy") * -1),
        )

    return df


def join_tracking_data():
    df_input = get_tracking_input()
    df_output = get_tracking_output()

    df_input = standardize_tracking_directions(df_input).drop(
        [
            "player_name",
            "player_position",
            "player_side",
            "player_height",
            "player_weight",
        ]
    )

    df_output = standardize_tracking_directions(
        df_output.join(
            df_input.select(
                ["game_id", "play_id", "nfl_id", "play_direction", "distance_to_goal"]
            ).unique(),
            on=["game_id", "play_id", "nfl_id"],
        )
    )
    df = (
        pl.concat(
            [
                df_input.with_columns(dataset=pl.lit("X")),
                df_output.with_columns(dataset=pl.lit("Y")),
            ],
            how="diagonal",
        )
    ).join(get_player_data(), on=["nfl_id"])

    return df


def add_tracking_features(tracking_df):
    over_cols = ["game_id", "play_id", "nfl_id"]

    return (
        tracking_df.sort(["game_id", "play_id", "nfl_id", "dataset", "frame_id"])
        .with_columns(
            frame_id=pl.row_index().over(
                ["game_id", "play_id", "nfl_id"], order_by=["dataset", "frame_id"]
            )
        )
        .with_columns(
            dx=pl.col("x").diff().over(over_cols, order_by="frame_id") / 0.1,
            dy=pl.col("y").diff().over(over_cols, order_by="frame_id") / 0.1,
        )
        .with_columns(s=(pl.col("dx") ** 2 + pl.col("dy") ** 2).sqrt())
        .with_columns(s_mph=pl.col("s") * (3600 / 1760))
        .with_columns(
            turn_angle=(
                pl.arctan2(
                    pl.col("y")
                    .shift(-1)
                    .over(["game_id", "play_id", "nfl_id"], order_by="frame_id")
                    - pl.col("y"),
                    pl.col("x")
                    .shift(-1)
                    .over(["game_id", "play_id", "nfl_id"], order_by="frame_id")
                    - pl.col("x"),
                )
                - pl.arctan2(
                    pl.col("y")
                    - pl.col("y")
                    .shift(1)
                    .over(["game_id", "play_id", "nfl_id"], order_by="frame_id"),
                    pl.col("x")
                    - pl.col("x")
                    .shift(1)
                    .over(["game_id", "play_id", "nfl_id"], order_by="frame_id"),
                )
            )
        )
        .with_columns(
            turn_angle=pl.when(
                (pl.col("turn_angle") > 3.14) | (pl.col("turn_angle") < -3.14)
            )
            .then(0)
            .otherwise(pl.col("turn_angle"))
        )
        .with_columns(prev_angle=pl.col("turn_angle").shift(1))
    )


def main(save_file=False):
    df = join_tracking_data()
    df = add_tracking_features(df)

    if save_file:
        df.write_parquet("analytics/prepped_data/combined_tracking.parquet")
        return None

    return df


if __name__ == "__main__":
    main(save_file=True)
