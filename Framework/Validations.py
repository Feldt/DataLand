import polars as pl


class Validation:
    def __init__(self):
        self

    def ValidateDatesAndKeys(df: pl.DataFrame) -> str:
        cols = (
            ",".join(map(str, df.to_series().to_list()))
            .replace("'", "")
            .replace("[[", "[")
            .replace("]]", "]")
        )

        keys = [
            f"{col}"
            for col in df.to_series().to_list()
            if len(col) < 11 and "id" in col.lower() or "date" in col.lower()
        ]
        # get diferent variables about dates in order to get valid date field
        # add more options or remove options according the needs
        dates = next(
            (
                col.replace("[", "").replace("]", "")
                for col in cols.split(",")
                if "dateupdated" == col.lower().replace("[", "").replace("]", "")
                or "CreationDate" == col.replace("[", "").replace("]", "")
                or "datecreated" == col.lower().replace("[", "").replace("]", "")
                or "date" == col.lower().replace("[", "").replace("]", "")
                or "StartDate" == col.replace("[", "").replace("]", "")
                # spanish and english in the same table
                and (
                    (
                        "fecha" == col.lower().replace("[", "").replace("]", "")
                        and "datecreated"
                        != col.lower().replace("[", "").replace("]", "")
                    )
                    or (
                        "creadofecha" == col.lower().replace("[", "").replace("]", "")
                        and "datecreated"
                        != col.lower().replace("[", "").replace("]", "")
                    )  # test dbpayroll.dbo.VisitantesTOG
                    or (
                        "fechacreacion" == col.lower().replace("[", "").replace("]", "")
                        and "datecreated"
                        != col.lower().replace("[", "").replace("]", "")
                    )
                )
            ),
            None,
        )
        return cols, keys, dates

    def ValidateIdentityColumn(df: pl.DataFrame) -> str:
        return next((col for col in df.to_series().to_list()), None)
