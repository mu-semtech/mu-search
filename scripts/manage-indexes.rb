#!/usr/bin/env ruby

require 'tty-prompt'
require 'typhoeus'
require 'json'


def get_indexes
  url = "#{BASE_URL}/indexes"
  response = Typhoeus.get(url, headers: { 'Accept' => 'application/json' })

  if response.success?
    JSON.parse(response.body)
  elsif response.timed_out?
    raise "Request timed out"
  elsif response.code == 0
    raise "Failed to connect: #{response.return_message}"
  else
    raise "Request failed. Status: #{response.code}, Response: #{response.body}"
  end
end

def delete_index(index, prompt)
  type = index['type']
  allowed_groups = index['allowed_groups']
  url = "#{BASE_URL}/#{type}"

  headers = {
    'Accept' => 'application/json',
    'Mu-Auth-Allowed-Groups' => JSON.generate(allowed_groups)
  }

  response = Typhoeus.delete(url, headers: headers)

  if response.success?
    prompt.say("Index successfully deleted")
    true
  elsif response.timed_out?
    prompt.say("Request timed out")
    false
  elsif response.code == 0
    prompt.say("Failed to connect: #{response.return_message}")
    false
  else
    prompt.say("Failed to delete index. Status: #{response.code}, Response: #{response.body}")
    false
  end
end

def update_index(index, prompt)
  type = index['type']
  allowed_groups = index['allowed_groups']
  url = "#{BASE_URL}/#{type}/index"

  headers = {
    'Accept' => 'application/json',
    'Mu-Auth-Allowed-Groups' => JSON.generate(allowed_groups)
  }

  response = Typhoeus.post(url, headers: headers)

  if response.success?
    result = JSON.parse(response.body)
    prompt.say("Index update initiated")
    prompt.say("Status: #{result['data'].map { |idx| "#{idx['id']}: #{idx['attributes']['status']}" }.join(', ')}")
    true
  elsif response.timed_out?
    prompt.say("Request timed out")
    false
  elsif response.code == 0
    prompt.say("Failed to connect: #{response.return_message}")
    false
  else
    prompt.say("Failed to update index. Status: #{response.code}, Response: #{response.body}")
    false
  end
end

prompt = TTY::Prompt.new
prompt.say("\n\n")
BASE_URL = prompt.ask("Enter base URL:", default: 'http://search')

indexes = get_indexes
options = indexes.map do |index|
  document_count = index['document_count']
  document_count = document_count.nil? ? 'N/A' : document_count
  {
    name: "#{index['type']} (#{index['name']}), \nAllowed Groups: #{index['allowed_groups']}, Document Count: #{document_count})",
    value: index
  }
end

# Display the options and let the user choose
chosen_index = prompt.select("Choose an index:", options)

# Show available actions for the chosen index
action = prompt.select("What would you like to do with this index?") do |menu|
  menu.choice 'Delete index', :delete
  menu.choice 'Update index (reindex)', :index
  menu.choice 'Exit', :exit
end

case action
when :delete
  if prompt.yes?("Are you sure you want to delete this index? This action cannot be undone.")
    delete_index(chosen_index, prompt)
  else
    prompt.say("Deletion cancelled")
  end
when :index
  if prompt.yes?("Are you sure you want to update this index?")
    update_index(chosen_index, prompt)
  else
    prompt.say("Update cancelled")
  end
when :exit
  prompt.say("Exiting...")
end
