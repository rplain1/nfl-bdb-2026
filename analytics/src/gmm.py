import polars as pl
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

from prep_data import get_player_data


def get_play_starts(df):
    return (
        df.filter(
            pl.all_horizontal(
                pl.col("player_side") == "Offense",
                pl.col("player_position") != "QB",
                pl.col("s") > 1,
            )
        )
        .group_by(["game_id", "play_id", "nfl_id"])
        .agg(pl.col("frame_id").min())
        .group_by(["game_id", "play_id"])
        .agg(frame_start=pl.col("frame_id").max())
    )


def create_distance_data(df):
    play_starts = get_play_starts(df)

    cols = [
        "game_id",
        "play_id",
        "nfl_id",
        "frame_id",
        "player_to_predict",
        "player_position",
        "player_side",
        "player_role",
        "x",
        "y",
        "o",
        "dir",
        "vx",
        "vy",
        "ox",
        "oy",
        "s",
        "s_mph",
    ]

    df_offense = (
        df.join(play_starts, on=["game_id", "play_id"], how="inner")
        .filter(
            pl.all_horizontal(
                pl.col("player_side") == "Offense",
                pl.col("dataset") == "X",
                pl.col("player_position").is_in(["WR", "TE", "RB"]),
                # pl.col("frame_id") > pl.col("frame_start"),
            )
        )
        .select(cols)
    )

    df_defense = (
        df.filter(
            pl.all_horizontal(
                pl.col("player_side") == "Defense",
                pl.col("dataset") == "X",
                pl.col("player_position").is_in(
                    ["CB", "S", "SS", "ILB", "FS", "LB", "OLB"]
                ),
            )
        )
        .select(cols)
        .with_columns(
            player_position=pl.when(pl.col("player_position").is_in(["S", "SS", "FS"]))
            .then(pl.lit("S"))
            .when(pl.col("player_position").is_in(["ILB", "LB", "OLB"]))
            .then(pl.lit("LB"))
            .otherwise(pl.col("player_position"))
        )
    )

    return df_offense.join(
        df_defense, on=["game_id", "play_id", "frame_id"], suffix="_def", how="inner"
    ).with_columns(
        distance=(
            (pl.col("x") - pl.col("x_def")).pow(2)
            + (pl.col("y") - pl.col("y_def")).pow(2)
        ).sqrt()
    )


def create_gmm_model_data(df_distances):
    over_cols = ["game_id", "play_id", "nfl_id", "nfl_id_def"]

    df_model = (
        df_distances.with_columns(
            min_dist=pl.min("distance").over(
                ["game_id", "play_id", "nfl_id", "nfl_id_def"]
            )
        )
        .filter(pl.col("min_dist") <= 12)
        .with_columns(y=pl.col("y") - (53.3 / 2), y_def=pl.col("y_def") - (53.3 / 2))
        .sort(by=over_cols + ["frame_id"])
        .with_columns(
            dist_change=(pl.col("distance") - pl.col("distance").shift(1)).abs(),
            min_dist_last_5=pl.col("distance").rolling_min(5).over(over_cols),
            delta_x=pl.col("x_def") - pl.col("x"),
            delta_y=pl.col("y_def") - pl.col("y"),
            mag_receiver=(pl.col("vx") ** 2 + pl.col("vy") ** 2).sqrt(),
            mag_defender=(pl.col("vx_def") ** 2 + pl.col("vy_def") ** 2).sqrt(),
            dot_prod=pl.col("vx") * pl.col("vx_def") + pl.col("vy") * pl.col("vy_def"),
        )
        .with_columns(
            velocity_alignment=pl.col("dot_prod")
            / (pl.col("mag_receiver") * pl.col("mag_defender")),
        )
        .with_columns(
            # Calculate relative velocity components
            rel_vx=pl.col("vx_def") - pl.col("vx"),
            rel_vy=pl.col("vy_def") - pl.col("vy"),
        )
        .with_columns(
            # Calculate the magnitude (relative speed)
            relative_speed=(pl.col("rel_vx") ** 2 + pl.col("rel_vy") ** 2).sqrt()
        )
        .select(
            [
                "game_id",
                "play_id",
                "frame_id",
                "nfl_id",
                "nfl_id_def",
                "player_position",
                "player_position_def",
                "distance",
                "dist_change",
                "min_dist_last_5",
                "delta_x",
                "delta_y",
                "velocity_alignment",
                "relative_speed",
            ]
        )
        .drop_nulls()
        .drop_nans()
    )

    df_model = pl.concat(
        [
            df_model.drop(["player_position", "player_position_def"]),
            df_model["player_position"].to_dummies(),
            df_model["player_position_def"].to_dummies(),
        ],
        how="horizontal",
    )

    return df_model


def create_coverage_assignment_probs(df_model):
    scaler = StandardScaler()
    X = scaler.fit_transform(
        df_model.drop(["game_id", "play_id", "frame_id", "nfl_id", "nfl_id_def"])
    )

    gm = GaussianMixture(n_components=2, random_state=52723).fit(X)
    player_data = get_player_data()
    return (
        pl.concat([df_model, pl.from_numpy(gm.predict_proba(X))], how="horizontal")
        .join(
            player_data.select("nfl_id", "player_name"),
            left_on=["nfl_id"],
            right_on="nfl_id",
        )
        .join(
            player_data.select("nfl_id", "player_name"),
            left_on=["nfl_id_def"],
            right_on="nfl_id",
            suffix="_def",
        )
    )


def main(save_file=False):
    df = pl.read_parquet("analytics/prepped_data/combined_tracking.parquet")
    df = create_distance_data(df)
    df = create_gmm_model_data(df)

    df = create_coverage_assignment_probs(df)

    if save_file:
        df.write_parquet("analytics/model_data/coverage_assignments.parquet")

    return df


if __name__ == "__main__":
    main(save_file=True)
