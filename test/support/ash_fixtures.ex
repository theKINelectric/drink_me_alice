defmodule DrinkMeAlice.Test.Crew do
  @moduledoc false
  use Ash.Domain, validate_config_inclusion?: false

  resources do
    resource DrinkMeAlice.Test.Crew.Mission
    resource DrinkMeAlice.Test.Crew.Task
  end
end

defmodule DrinkMeAlice.Test.Crew.Mission do
  @moduledoc false
  use Ash.Resource,
    domain: DrinkMeAlice.Test.Crew,
    data_layer: DrinkMeAlice.Ash.DataLayer

  surrealdb do
    conn(DrinkMeAlice.Test.Surreal)
    table "mission"
    max_rows(25)
  end

  attributes do
    uuid_primary_key :id, writable?: true
    attribute :title, :string, allow_nil?: false, public?: true

    attribute :status, :atom,
      constraints: [one_of: [:draft, :active, :done]],
      default: :draft,
      public?: true

    attribute :priority, :integer, public?: true
    attribute :due_at, :utc_datetime_usec, public?: true
    attribute :budget, :decimal, public?: true
    attribute :score, :float, public?: true
    attribute :tags, {:array, :string}, public?: true
    attribute :meta, :map, public?: true
    attribute :on_day, :date, public?: true
  end

  identities do
    identity :unique_title, [:title], pre_check?: false
  end

  relationships do
    has_many :tasks, DrinkMeAlice.Test.Crew.Task,
      destination_attribute: :mission_id,
      public?: true
  end

  actions do
    defaults [:read, :destroy, create: :*, update: :*]
  end
end

defmodule DrinkMeAlice.Test.Crew.Task do
  @moduledoc false
  use Ash.Resource,
    domain: DrinkMeAlice.Test.Crew,
    data_layer: DrinkMeAlice.Ash.DataLayer

  surrealdb do
    conn(DrinkMeAlice.Test.Surreal)
    table "task"
  end

  attributes do
    uuid_primary_key :id
    attribute :name, :string, allow_nil?: false, public?: true
    attribute :done, :boolean, default: false, allow_nil?: false, public?: true
  end

  relationships do
    belongs_to :mission, DrinkMeAlice.Test.Crew.Mission,
      allow_nil?: false,
      public?: true,
      attribute_writable?: true
  end

  actions do
    defaults [:read, :destroy, create: :*, update: :*]
  end
end
