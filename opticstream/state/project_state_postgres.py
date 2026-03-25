"""
Postgres-backed project-state repository.

This repository persists a full project state model as JSON in a generic
`project_state` table keyed by (project_type, project_name).
"""

from __future__ import annotations

import re
from typing import Generic

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

from opticstream.state.project_state_core import TState


class PostgresProjectStateRepository(Generic[TState]):
    """DB-backed repository for project state models."""

    def __init__(
        self,
        block_name: str,
        model_cls: type[TState],
        project_type: str,
        table_name: str = "project_state",
    ) -> None:
        self._block_name = block_name
        self._model_cls = model_cls
        self._project_type = project_type
        self._table_name = self._validate_table_name(table_name)

    def load(self, project_name: str) -> TState:
        # Import locally so module import remains lightweight in environments
        # where this repository is configured but not used.
        from prefect_sqlalchemy import SqlAlchemyConnector  # type: ignore[import-not-found]

        query = text(
            f"""
            SELECT state
            FROM {self._table_name}
            WHERE project_type = :project_type
              AND project_name = :project_name
            """
        )

        database_block = SqlAlchemyConnector.load(self._block_name)
        with database_block:
            with database_block.get_connection(begin=False) as connection:
                row = connection.execute(
                    query,
                    {
                        "project_type": self._project_type,
                        "project_name": project_name,
                    },
                ).fetchone()

        if row is None:
            # type: ignore[call-arg] - callers must ensure model_cls can default construct
            return self._model_cls()  # type: ignore[return-value]

        return self._model_cls.model_validate(row[0])

    def save(self, project_name: str, state: TState) -> None:
        from prefect_sqlalchemy import SqlAlchemyConnector  # type: ignore[import-not-found]

        payload = state.model_dump(mode="json")
        query = text(
            f"""
            INSERT INTO {self._table_name} (project_type, project_name, state)
            VALUES (:project_type, :project_name, :state)
            ON CONFLICT (project_type, project_name)
            DO UPDATE SET state = EXCLUDED.state
            """
        ).bindparams(
            bindparam("state", type_=JSONB),
        )

        database_block = SqlAlchemyConnector.load(self._block_name)
        with database_block:
            with database_block.get_connection(begin=True) as connection:
                connection.execute(
                    query,
                    {
                        "project_type": self._project_type,
                        "project_name": project_name,
                        "state": payload,
                    },
                )

    @staticmethod
    def _validate_table_name(table_name: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
            raise ValueError(f"Invalid SQL table name: {table_name!r}")
        return table_name
