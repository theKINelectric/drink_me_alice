defmodule DrinkMeAlice.AshDataLayerTest do
  @moduledoc """
  The Ash data layer against a real pinned SurrealDB 3.0.5: schema from
  resources, typed round-trips, pushed-down filters/sorts/pagination, the
  read bound, honest write errors, and relationship loading.
  """
  use ExUnit.Case, async: false

  @moduletag :live

  require Ash.Query
  alias DrinkMeAlice.{Admin, Session}
  alias DrinkMeAlice.Ash.Schema
  alias DrinkMeAlice.Test.Crew.{Mission, Task}
  alias DrinkMeAlice.Test.Live

  setup_all do
    live = Live.start!(ns: "ash", db: "ash")
    :ok = Admin.migrate(live.root, [Schema.migration(1, "crew", [Mission, Task])])
    conn = Live.start_conn!(live, name: DrinkMeAlice.Test.Surreal)
    on_exit(fn -> Live.stop!(live) end)
    %{srv: live, conn: conn}
  end

  setup %{srv: live} do
    {:ok, s} = Session.open(live.root)
    {:ok, s, _} = Session.query(s, "DELETE mission; DELETE task;")
    Session.close(s)
    :ok
  end

  defp mission!(attrs), do: Mission |> Ash.Changeset.for_create(:create, attrs) |> Ash.create!()

  describe "schema" do
    test "DDL is generated from the resource, pk as record id, identities as unique indexes" do
      {:ok, stmts} = Schema.statements(Mission)
      assert "DEFINE TABLE mission SCHEMAFULL" in stmts
      assert "DEFINE FIELD title ON TABLE mission TYPE string" in stmts
      assert "DEFINE FIELD due_at ON TABLE mission TYPE option<datetime>" in stmts
      assert "DEFINE FIELD budget ON TABLE mission TYPE option<decimal>" in stmts
      assert "DEFINE FIELD tags ON TABLE mission TYPE option<array<string>>" in stmts
      # Maps are exact canonical JSON text (nested strings would be coerced as objects).
      assert "DEFINE FIELD meta ON TABLE mission TYPE option<string>" in stmts
      assert "DEFINE INDEX mission_unique_title_uq ON TABLE mission FIELDS title UNIQUE" in stmts
      refute Enum.any?(stmts, &String.contains?(&1, "FIELD id "))
    end
  end

  describe "create + read" do
    test "every supported type round-trips exactly" do
      due = ~U[2026-09-24 12:34:56.123456Z]

      m =
        mission!(%{
          title: "Survey the reef",
          status: :active,
          priority: 3,
          due_at: due,
          budget: Decimal.new("1234.50"),
          score: 0.75,
          tags: ["dive", "0b8e8c1a-1111-4000-8000-000000000009"],
          meta: %{"depth_m" => 30, "crew" => ["a", "b"]},
          on_day: ~D[2026-10-01]
        })

      [back] = Mission |> Ash.Query.filter(id == ^m.id) |> Ash.read!()

      assert back.id == m.id
      assert back.title == "Survey the reef"
      assert back.status == :active
      assert back.priority == 3
      assert back.due_at == due
      assert Decimal.equal?(back.budget, Decimal.new("1234.50"))
      assert back.score == 0.75
      assert back.tags == ["dive", "0b8e8c1a-1111-4000-8000-000000000009"]
      assert back.meta == %{"depth_m" => 30, "crew" => ["a", "b"]}
      assert back.on_day == ~D[2026-10-01]
    end

    test "nil attributes are stored as NONE and read back as nil" do
      m = mission!(%{title: "Bare"})
      [back] = Ash.read!(Mission)
      assert back.id == m.id

      assert back.due_at == nil and back.budget == nil and back.tags == nil and
               back.priority == nil

      assert back.status == :draft
    end

    test "Ash.get! by primary key" do
      m = mission!(%{title: "Find me"})
      assert Ash.get!(Mission, m.id).title == "Find me"
    end
  end

  describe "filters, sort, pagination" do
    setup do
      for {t, p, s} <- [{"a", 1, :draft}, {"b", 2, :active}, {"c", 3, :active}, {"d", nil, :done}] do
        mission!(%{title: t, priority: p, status: s})
      end

      :ok
    end

    defp titles(query), do: query |> Ash.read!() |> Enum.map(& &1.title) |> Enum.sort()

    test "comparison, in, is_nil, and/or/not are pushed down" do
      assert titles(Ash.Query.filter(Mission, priority > 1)) == ["b", "c"]
      assert titles(Ash.Query.filter(Mission, priority <= 2)) == ["a", "b"]
      assert titles(Ash.Query.filter(Mission, status in [:draft, :done])) == ["a", "d"]
      assert titles(Ash.Query.filter(Mission, is_nil(priority))) == ["d"]

      assert titles(Ash.Query.filter(Mission, not is_nil(priority) and status == :active)) == [
               "b",
               "c"
             ]

      assert titles(Ash.Query.filter(Mission, title == "a" or priority == 3)) == ["a", "c"]
      assert titles(Ash.Query.filter(Mission, not (status == :active))) == ["a", "d"]
      assert titles(Ash.Query.filter(Mission, title != "a")) == ["b", "c", "d"]
    end

    test "comparisons with an absent value are UNKNOWN (SQL three-valued logic), even under not" do
      # "d" has no priority: in SurrealQL `NONE <= 2` and `!(NONE > 1)` are
      # both true; in Ash/SQL they are unknown and the row is excluded.
      assert titles(Ash.Query.filter(Mission, priority <= 2)) == ["a", "b"]
      assert titles(Ash.Query.filter(Mission, priority != 1)) == ["b", "c"]
      assert titles(Ash.Query.filter(Mission, not (priority > 1))) == ["a"]
      # Kleene: for "d", UNKNOWN AND FALSE is FALSE, so its negation is TRUE.
      assert titles(Ash.Query.filter(Mission, not (priority > 1 and status == :active))) == [
               "a",
               "d"
             ]

      assert titles(Ash.Query.filter(Mission, not (priority > 1) or is_nil(priority))) == [
               "a",
               "d"
             ]
    end

    test "sort, limit and offset become ORDER BY / LIMIT / START" do
      page =
        Mission
        |> Ash.Query.sort(title: :desc)
        |> Ash.Query.limit(2)
        |> Ash.Query.offset(1)
        |> Ash.read!()

      assert Enum.map(page, & &1.title) == ["c", "b"]
    end

    test "an unsupported filter is refused, never evaluated over a full fetch" do
      assert {:error, error} = Mission |> Ash.Query.filter(contains(title, "a")) |> Ash.read()
      assert Exception.message(error) =~ ~r/contains|not support/i
    end

    test "reads are bounded: more rows than max_rows is an error, not a truncation" do
      for i <- 1..22, do: mission!(%{title: "bulk-#{i}"})
      # 4 + 22 = 26 rows > max_rows 25.
      assert {:error, error} = Ash.read(Mission)
      assert Exception.message(error) =~ "bounded_limit_exceeded"

      assert {:error, _} = Mission |> Ash.Query.limit(26) |> Ash.read()
      assert length(Mission |> Ash.Query.limit(25) |> Ash.read!()) == 25
    end
  end

  describe "update + destroy" do
    test "update changes only the changed fields" do
      m = mission!(%{title: "Old", priority: 1, tags: ["x"]})
      u = m |> Ash.Changeset.for_update(:update, %{title: "New", priority: nil}) |> Ash.update!()
      assert u.title == "New" and u.priority == nil and u.tags == ["x"]
      assert Ash.get!(Mission, m.id).title == "New"
    end

    test "update and destroy of a vanished record are StaleRecord, checked inside the tx", %{
      srv: live
    } do
      m = mission!(%{title: "Ghost"})
      {:ok, s} = Session.open(live.root)
      {:ok, s, _} = Session.query(s, "DELETE mission;")
      Session.close(s)

      assert {:error, %Ash.Error.Invalid{errors: [%Ash.Error.Changes.StaleRecord{}]}} =
               m |> Ash.Changeset.for_update(:update, %{title: "Boo"}) |> Ash.update()

      assert {:error, %Ash.Error.Invalid{errors: [%Ash.Error.Changes.StaleRecord{}]}} =
               Ash.destroy(m)
    end

    test "destroy removes the record" do
      m = mission!(%{title: "Gone"})
      :ok = Ash.destroy!(m)
      assert Ash.read!(Mission) == []
    end
  end

  describe "write errors" do
    test "a duplicate primary key is 'has already been taken', not an unknown outcome" do
      id = Ash.UUID.generate()
      mission!(%{id: id, title: "First"})

      assert {:error,
              %Ash.Error.Invalid{errors: [%Ash.Error.Changes.InvalidChanges{fields: [:id]}]}} =
               Mission
               |> Ash.Changeset.for_create(:create, %{id: id, title: "Second"})
               |> Ash.create()
    end

    test "an identity (unique index) violation names the identity's fields" do
      mission!(%{title: "Unique"})

      assert {:error,
              %Ash.Error.Invalid{errors: [%Ash.Error.Changes.InvalidChanges{fields: [:title]}]}} =
               Mission |> Ash.Changeset.for_create(:create, %{title: "Unique"}) |> Ash.create()
    end

    test "a store that is down is Unavailable (certified unsent), not a crash", %{srv: live} do
      {:ok, pid} =
        DrinkMeAlice.Conn.start(
          live.runtime
          |> Map.to_list()
          |> Keyword.merge(port: 1, name: :bs_nowhere, poll_ms: 5)
        )

      try do
        assert {:error, {:refused, _}} =
                 DrinkMeAlice.query(:bs_nowhere, "RETURN 1;", %{}, deadline_ms: 50)
      after
        GenServer.stop(pid)
      end
    end
  end

  describe "relationships" do
    test "has_many and belongs_to load through pushed-down `in` filters" do
      m1 = mission!(%{title: "M1"})
      m2 = mission!(%{title: "M2"})

      for {name, m} <- [{"t1", m1}, {"t2", m1}, {"t3", m2}] do
        Task
        |> Ash.Changeset.for_create(:create, %{name: name, mission_id: m.id})
        |> Ash.create!()
      end

      [l1, l2] = Mission |> Ash.Query.sort(title: :asc) |> Ash.Query.load(:tasks) |> Ash.read!()
      assert l1.tasks |> Enum.map(& &1.name) |> Enum.sort() == ["t1", "t2"]
      assert Enum.map(l2.tasks, & &1.name) == ["t3"]

      t3 = Task |> Ash.Query.filter(name == "t3") |> Ash.Query.load(:mission) |> Ash.read_one!()
      assert t3.mission.id == m2.id
      assert t3.done == false
    end
  end
end
