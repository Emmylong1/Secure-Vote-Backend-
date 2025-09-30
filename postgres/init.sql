create table if not exists elections (
  id bigint primary key,
  name text not null
);

create table if not exists candidates (
  id bigint primary key,
  election_id bigint not null references elections(id) on delete cascade,
  name text not null
);

create table if not exists voters (
  id text primary key,
  voter_card_hash text not null
);

create table if not exists votes (
  id bigserial primary key,
  election_id bigint not null references elections(id) on delete cascade,
  candidate_id bigint not null references candidates(id) on delete cascade,
  voter_id text not null references voters(id) on delete cascade,
  created_at timestamptz not null default now(),
  constraint uniq_one_vote unique (election_id, voter_id)
);

create table if not exists candidate_tally (
  election_id bigint not null,
  candidate_id bigint not null,
  votes bigint not null default 0,
  primary key (election_id, candidate_id),
  foreign key (election_id) references elections(id) on delete cascade,
  foreign key (candidate_id) references candidates(id) on delete cascade
);

create or replace function notify_vote() returns trigger as $$
begin
  perform pg_notify('vote_inserted', json_build_object(
    'election_id', NEW.election_id,
    'candidate_id', NEW.candidate_id
  )::text);
  return NEW;
end;
$$ language plpgsql;

drop trigger if exists trg_notify_vote on votes;
create trigger trg_notify_vote after insert on votes
for each row execute function notify_vote();

create or replace function bump_tally() returns trigger as $$
begin
  insert into candidate_tally(election_id, candidate_id, votes)
  values (NEW.election_id, NEW.candidate_id, 1)
  on conflict (election_id, candidate_id) do update
    set votes = candidate_tally.votes + 1;
  return NEW;
end;
$$ language plpgsql;

drop trigger if exists trg_bump_tally on votes;
create trigger trg_bump_tally after insert on votes
for each row execute function bump_tally();

-- seed demo data
insert into elections(id, name) values (1, 'SecureVote 2025') on conflict do nothing;
insert into candidates(id, election_id, name) values
  (101, 1, 'John Smith'),
  (102, 1, 'Sarah Johnson'),
  (103, 1, 'Michael Chen')
on conflict do nothing;
