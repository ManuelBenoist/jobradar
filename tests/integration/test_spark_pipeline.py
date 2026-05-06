import pytest

from src.spark.transform import (
    SILVER_OUTPUT_COLUMNS,
    apply_silver_logic,
    create_spark_session,
    validate_columns,
)


@pytest.mark.integration
class TestSparkPipelineIntegration:
    @pytest.fixture(scope="class")
    def spark(self):
        session = create_spark_session()
        yield session
        session.stop()

    def test_validate_columns_passes_with_all_columns(self, spark):
        df = spark.createDataFrame([(1, "a")], ["id", "name"])
        validate_columns(df, ["id", "name"], "test")
        assert True

    def test_validate_columns_raises_on_missing(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        with pytest.raises(ValueError, match="Colonnes obligatoires manquantes"):
            validate_columns(df, ["id", "missing_col"], "test")

    def test_validate_columns_raises_on_empty_df(self, spark):
        df = spark.createDataFrame([], schema="id INT")
        with pytest.raises(ValueError):
            validate_columns(df, ["id", "nope"], "test")

    def test_apply_silver_logic_produces_all_output_columns(self, spark):
        columns = [
            "job_id", "title", "company_name", "description", "location",
            "salary_info", "created_at", "url", "source_name",
        ]
        data = [(
            "1", "Data Engineer", "Test Corp", "A great job in Nantes using Python and AWS.",
            "Nantes", "45k", "2025-01-01T00:00:00Z", "https://example.com/job/1", "Adzuna",
        )]
        df = spark.createDataFrame(data, columns)
        result = apply_silver_logic(df)

        actual_cols = set(result.columns)
        for col in SILVER_OUTPUT_COLUMNS:
            assert col in actual_cols, f"Colonne manquante dans la sortie Silver: {col}"

    def test_apply_silver_logic_deduplicates_identical_jobs(self, spark):
        columns = [
            "job_id", "title", "company_name", "description", "location",
            "salary_info", "created_at", "url", "source_name",
        ]
        data = [
            ("1", "Data Engineer", "Test Corp", "Job description A.", "Nantes", "50k", "2025-01-01T00:00:00Z", "https://ex.com/1", "Adzuna"),
            ("2", "Data Engineer", "Test Corp", "Job description A.", "Nantes", "50k", "2025-01-02T00:00:00Z", "https://ex.com/2", "Jooble"),
        ]
        df = spark.createDataFrame(data, columns)
        result = apply_silver_logic(df)

        assert result.count() == 1, "La déduplication devrait réduire à 1 offre"

    def test_apply_silver_logic_keeps_latest_on_dup(self, spark):
        columns = [
            "job_id", "title", "company_name", "description", "location",
            "salary_info", "created_at", "url", "source_name",
        ]
        data = [
            ("old", "Data Engineer", "Corp", "Desc", "Paris", "", "2024-01-01", "https://ex.com/old", "Adzuna"),
            ("new", "Data Engineer", "Corp", "Desc", "Paris", "", "2025-01-01", "https://ex.com/new", "Jooble"),
        ]
        df = spark.createDataFrame(data, columns)
        result = apply_silver_logic(df)
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["job_id"] == "new", "La déduplication devrait garder l'offre la plus récente"

    def test_apply_silver_logic_sets_data_quality_score(self, spark):
        columns = [
            "job_id", "title", "company_name", "description", "location",
            "salary_info", "created_at", "url", "source_name",
        ]
        data = [
            ("1", "DE FT", "Corp FT", "Desc FT", "Paris 44", "", "2025-01-01", "https://ex.com/1", "France Travail"),
            ("2", "DE JS", "Corp JS", "Desc JS", "Paris 75", "", "2025-01-01", "https://ex.com/2", "JSearch"),
            ("3", "DE AZ", "Corp AZ", "Desc AZ", "Lyon 69", "", "2025-01-01", "https://ex.com/3", "Adzuna"),
            ("4", "DE JB", "Corp JB", "Desc JB", "Nantes 44", "", "2025-01-01", "https://ex.com/4", "Jooble"),
        ]
        df = spark.createDataFrame(data, columns)
        result = apply_silver_logic(df)
        rows = {r["job_id"]: r for r in result.collect()}

        assert rows["1"]["data_quality_score"] == 1.0
        assert rows["2"]["data_quality_score"] == 1.0
        assert rows["3"]["data_quality_score"] == 0.75
        assert rows["4"]["data_quality_score"] == 0.75

    def test_apply_silver_logic_extracts_skills(self, spark):
        columns = [
            "job_id", "title", "company_name", "description", "location",
            "salary_info", "created_at", "url", "source_name",
        ]
        data = [(
            "1", "Data Engineer", "Corp",
            "We use Python, AWS Lambda, S3, and Spark for our data pipeline.",
            "Paris", "", "2025-01-01", "https://ex.com/1", "Adzuna",
        )]
        df = spark.createDataFrame(data, columns)
        result = apply_silver_logic(df)
        row = result.collect()[0]

        skills = row["extracted_skills"]
        assert "python" in skills
        assert "spark" in skills

    def test_apply_silver_logic_marks_junior_and_remote(self, spark):
        columns = [
            "job_id", "title", "company_name", "description", "location",
            "salary_info", "created_at", "url", "source_name",
        ]
        data = [(
            "1", "Junior Data Engineer", "Corp",
            "Great junior position with remote work possible.",
            "Nantes", "", "2025-01-01", "https://ex.com/1", "Adzuna",
        )]
        df = spark.createDataFrame(data, columns)
        result = apply_silver_logic(df)
        row = result.collect()[0]

        assert row["is_junior"] is True
        assert row["is_red_flag"] is False
