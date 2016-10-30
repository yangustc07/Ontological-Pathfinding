#!/bin/bash
#
# This script runs the Ontological Pathfinding algorithm. It assumes that the
# algorithm has been compiled into a jar package 
# "Ontological-Pathfinding-assembly-1.0.jar" with the main class "Main," and
# the binary "spark-submit" is available to launch the application.
# 
# Input includes facts, schema, and initial rules.
#
# op/data/YAGOData/YAGOFacts.csv
# op/data/YAGOData/YAGORules.csv-[1-6]
# op/data/YAGOData/YAGOSchema.csv
# op/run.sh
#
# The output rules and facts will be stored in the directories specified by
# ${OUTPUT_RULES}/k and ${OUTPUT_FACTS}/k where k is the rule type.
#
# Please run the script at the project root directory ("op" in the example
# above):
#
# $./run.sh
# 
# Yang Chen
# yang@cise.ufl.edu

# Input and output in string representation.
INPUT_FACTS=data/YAGOData/YAGOFacts.csv

# Rules are supposed to be named ${INPUT_RULES_PREFIX}-$i.
INPUT_RULES_PREFIX=data/YAGOData/YAGORules.csv
INPUT_SCHEMA=data/YAGOData/YAGOSchema.csv

# Total number of rule types.
INPUT_RULES_TYPES=6

# Output directories.
OUTPUT_RULES=output-rules
OUTPUT_FACTS=output-facts

# Partitioning algorithm parameters.
MAX_FACTS=2000000
MAX_RULES=1000

# Rule threshold for inference.
MIN_SUPPORT=2
MIN_CONFIDENCE=0.6

# Needs to translate strings to integers? For the first run, set them to
# 'true.' It will generate a set of '.map' files. With previous '.map' files,
# it is ok to set them to 'false' in subsequent runs.
MAP_PREDICATES=true
MAP_ENTITIES=true

# App configuration.
SPARK_PATH=${HOME}/spark-1.5.1/bin/spark-submit
JAR_PATH=target/scala-2.10/Ontological-Pathfinding-assembly-1.0.jar
MAIN_CLASS=Main
NCORES=64
DRIVER_MEMORY=400G
EXECUTOR_MEMORY=100G

# For candidate rule construction only.
POSTGRESQL_BIN=  # default to /usr/local/pgsql/bin
POSTGRESQL_HOST=localhost
POSTGRESQL_USER=op
POSTGRESQL_DB=op

# Intermediate results.
MINING_PARTITION_OUTPUT=mining-partition-output
MINING_OUTPUT=mining-output
INFERENCE_PARTITION_OUTPUT=inference-partition-output
INFERENCE_OUTPUT=inference-output

function run() {
  print_info "Ontological Pathfinding"
  print_info "1. Rule mining."
  print_info "2. Knowledge expansion."
  read -p "Choice: [1/2/q]" -n 1 -r
  if [[ $REPLY =~ ^[1]$ ]]; then
    echo  # Prints newline
    run_mine
  elif [[ $REPLY =~ ^[2]$ ]]; then
    echo
    run_infer
  else
    echo
    exit 0
  fi
}

# Runs rule mining algorithm.
function run_mine() {
  validate_mining_input

  # Maps string representation to integers.
  if [ "${MAP_PREDICATES}" = true ]; then
    message="Mapping facts file \"${INPUT_FACTS}\" to integer representation."
    print_info "${message}"
    map_facts_to_int "${INPUT_FACTS}" "${INPUT_SCHEMA}" "${INPUT_FACTS}.map" \
                     "${MAP_ENTITIES}"
  fi

  for rule_type in $(seq 1 ${INPUT_RULES_TYPES}); do
    if [ "${MAP_PREDICATES}" = true ]; then
      message="Mapping rules file \"${INPUT_RULES_PREFIX}-${rule_type}\" to "
      message+="integer representation."
      print_info "${message}"
      map_rules_to_int "${INPUT_RULES_PREFIX}-${rule_type}" "${rule_type}" \
                       "${INPUT_RULES_PREFIX}-${rule_type}.map"
    fi

    # Partitions the input KB.
    message="Partitioning KB \"${INPUT_FACTS}\" and "
    message+="\"${INPUT_RULES_PREFIX}-${rule_type}\" into subsets with "
    message+="max facts = ${MAX_FACTS} and max rules = ${MAX_RULES}."
    print_info "${message}"
    run_partitioning "${INPUT_FACTS}.map" \
                     "${INPUT_RULES_PREFIX}-${rule_type}.map" \
                     "${MINING_PARTITION_OUTPUT}/${rule_type}" \
                     "${MAX_FACTS}" "${MAX_RULES}"

    # Runs mining algorithm for each partition.
    message="Mining rules from \"${INPUT_FACTS}\" and "
    message+="\"${INPUT_RULES_PREFIX}-${rule_type}.\""
    print_info "${message}"
    num_parts=$(ls -l ${MINING_PARTITION_OUTPUT}/${rule_type} | \
      grep 'part-' | wc -l)
    for i in $(seq 0 $((num_parts-1))); do
      print_info "Mining partition $((i+1))/${num_parts}."
      run_mining \
        "${MINING_PARTITION_OUTPUT}/${rule_type}/part-$i/facts.csv/"'*' \
        "${MINING_PARTITION_OUTPUT}/${rule_type}/part-$i/rules.csv" \
        "${rule_type}" 100 \
        "${MINING_OUTPUT}/${rule_type}/part-$i" \
        "${MINING_OUTPUT}/${rule_type}/part-$i/rules" \
        "${MINING_OUTPUT}/${rule_type}/part-$i/prune"
    done
    cat "${MINING_OUTPUT}/${rule_type}"/part-*/rules | sort -k1,1 \
        > "${MINING_OUTPUT}/${rule_type}/rules"
    cat "${MINING_OUTPUT}/${rule_type}"/part-*/prune | sort -k1,1 \
        > "${MINING_OUTPUT}/${rule_type}/prune"

    # Format rules for output.
    print_info "Writing output rules to \"${OUTPUT_RULES}/${rule_type}.\""
    reformat_rules "${INPUT_RULES_PREFIX}-${rule_type}.map" \
                   "${MINING_OUTPUT}/${rule_type}/rules" \
                   "${MINING_OUTPUT}/${rule_type}/prune" ${rule_type} \
                   "${OUTPUT_RULES}/${rule_type}/rules" \
                   "${OUTPUT_RULES}/${rule_type}/prune"
  done
  print_info "Rule mining finishes."
}

# Run the inference algorithm.
function run_infer() {
  validate_inference_input
  for rule_type in $(seq 1 ${INPUT_RULES_TYPES}); do
    # Prepare rules for inference according to "MIN_SUPPORT" and
    # "MIN_CONFIDENCE."
    awk '{if ($2>='"${MIN_SUPPORT}"' && $3>='"${MIN_CONFIDENCE}"') print $1}' \
        "${MINING_OUTPUT}/${rule_type}/rules" > \
        "${MINING_OUTPUT}/${rule_type}/rules.infer"
    join "${INPUT_RULES_PREFIX}-${rule_type}.map" \
         "${MINING_OUTPUT}/${rule_type}/rules.infer" > \
         "${INPUT_RULES_PREFIX}-${rule_type}.infer"

    # Partitions the input KB.
    message="Partitioning KB \"${INPUT_FACTS}\" and "
    message+="\"${INPUT_RULES_PREFIX}-${rule_type}\" into subsets with "
    message+="max facts = ${MAX_FACTS} and max rules = ${MAX_RULES}."
    print_info "${message}"
    run_partitioning "${INPUT_FACTS}.map" \
                     "${INPUT_RULES_PREFIX}-${rule_type}.infer" \
                     "${INFERENCE_PARTITION_OUTPUT}/${rule_type}" \
                     "${MAX_FACTS}" "${MAX_RULES}"

    # Runs mining algorithm for each partition.
    message="Inferring facts from \"${INPUT_FACTS}\" and "
    message+="\"${INPUT_RULES_PREFIX}-${rule_type}.\""
    print_info "${message}"
    num_parts=$(ls -l ${INFERENCE_PARTITION_OUTPUT}/${rule_type} \
              | grep 'part-' | wc -l)
    for i in $(seq 0 $((num_parts-1))); do
      print_info "Inferring from partition $((i+1))/${num_parts}."
      run_inference \
        "${INFERENCE_PARTITION_OUTPUT}/${rule_type}/part-$i/facts.csv/"'*' \
        "${INFERENCE_PARTITION_OUTPUT}/${rule_type}/part-$i/rules.csv" \
        "${rule_type}" \
        "${INFERENCE_OUTPUT}/${rule_type}/part-$i"
      done
    cat "${INFERENCE_OUTPUT}/${rule_type}"/part-*/part-* | sort -t':' -k2,2 \
        > "${INFERENCE_OUTPUT}/${rule_type}/facts"

    # Format facts for output.
    print_info "Writing inferred facts to \"${OUTPUT_FACTS}/${rule_type}.\""
    reformat_facts "${INFERENCE_OUTPUT}/${rule_type}/facts" \
                   "${OUTPUT_RULES}/${rule_type}/rules" \
                   "${OUTPUT_FACTS}/${rule_type}/facts" \
                   "$(dirname "${INPUT_FACTS}")/predicates.map" \
                   "$(dirname "${INPUT_FACTS}")/entities.map"
  done
  print_info "Inference finishes."
}

###############################################################################
# Validates input programs.
###############################################################################
function validate_spark() {
  if [ ! -e "${SPARK_PATH}" ]; then
    print_error_exit "Invalid Spark binary \"${SPARK_PATH}.\""
  fi

  if [ ! -e "${JAR_PATH}" ]; then
    message="Invalid jar \"${JAR_PATH}.\"\n"
    message+="Please make sure you have run \"sbt assembly.\""
    print_error_exit "${message}"
  fi
}

###############################################################################
# Validates input files.
###############################################################################
function validate_mining_input() {
  validate_spark
  if [ ! -e "${INPUT_FACTS}" ]; then
    print_error_exit "Invalid input facts file \"${INPUT_FACTS}.\""
  fi

  if [ ! -e "${INPUT_SCHEMA}" ]; then
    read -p "Construct universal schema from ${INPUT_FACTS}? [y/n]" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
      construct_schema
    else
      print_error_exit "Invalid input schema file \"${input_schema}.\""
    fi
  fi

  for rule_type in $(seq 1 ${INPUT_RULES_TYPES}); do
    if [ ! -e "${INPUT_RULES_PREFIX}-${rule_type}" ]; then
      print_info "Construct candidate rules into \"${INPUT_RULES_PREFIX}-i.\""
      if [ ! -e "op.sql" ]; then
        print_error_exit "SQL script \"op.sql\" not found."
      else
        construct_candidate_rules
      fi
      break
    fi
  done

  if [ -e "${OUTPUT_RULES}" ]; then
    print_info "Output directory ${OUTPUT_RULES} exists."
    read -p "Clear ${OUTPUT_RULES}? [y/n]" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
      rm -rf ${OUTPUT_RULES}/*
    else
      print_error_exit "Exit."
    fi
  fi

  if [ -e "${MINING_PARTITION_OUTPUT}" ]; then
    print_info "Partitioning directory ${MINING_PARTITION_OUTPUT} exists."
    read -p "Clear ${MINING_PARTITION_OUTPUT}? [y/n]" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
      rm -rf ${MINING_PARTITION_OUTPUT}
    else
      print_error_exit "Exit."
    fi
  fi

  if [ -e "${MINING_OUTPUT}" ]; then
    print_info "Mining directory ${MINING_OUTPUT} exists."
    read -p "Clear ${MINING_OUTPUT}? [y/n]" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
      rm -rf ${MINING_OUTPUT}
    else
      print_error_exit "Exit."
    fi
  fi
}

function validate_inference_input() {
  validate_spark
  if [ ! -e "${INPUT_FACTS}" ]; then
    print_error_exit "Invalid input facts file \"${INPUT_FACTS}.\""
  fi

  for rule_type in $(seq 1 ${INPUT_RULES_TYPES}); do
    if [ ! -e "${MINING_OUTPUT}/${rule_type}/rules" ]; then
      message="Invalid input rules file ${MINING_OUTPUT}/${rule_type}/rules."
      print_error_exit "${message}"
    fi
    if [ ! -e "${OUTPUT_RULES}/${rule_type}/rules" ]; then
      message="Invalid input rules file ${OUTPUT_RULES}/${rule_type}/rules."
      print_error_exit "${message}"
    fi
  done

  if [ -e "${OUTPUT_FACTS}" ]; then
    print_info "Output directory ${OUTPUT_FACTS} exists."
    read -p "Clear ${OUTPUT_FACTS}? [y/n]" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
      rm -rf ${OUTPUT_FACTS}/*
    else
      print_error_exit "Exit."
    fi
  fi

  if [ -e "${INFERENCE_PARTITION_OUTPUT}" ]; then
    print_info "Partitioning directory ${INFERENCE_PARTITION_OUTPUT} exists."
    read -p "Clear ${INFERENCE_PARTITION_OUTPUT}? [y/n]" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
      rm -rf ${INFERENCE_PARTITION_OUTPUT}
    else
      print_error_exit "Exit."
    fi
  fi

  if [ -e "${INFERENCE_OUTPUT}" ]; then
    print_info "Inference directory ${INFERENCE_OUTPUT} exists."
    read -p "Clear ${INFERENCE_OUTPUT}? [y/n]" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
      rm -rf ${INFERENCE_OUTPUT}
    else
      print_error_exit "Exit."
    fi
  fi
}

###############################################################################
# Prints a message.
# Arguments:
#   $1: Message to print.
###############################################################################
function print_info() {
  echo -e "$(date +%T) \e[32m[INFO]\e[0m $1"
}

###############################################################################
# Prints an error message and exit.
# Arguments:
#   $1: Error message to print.
###############################################################################
function print_error_exit() {
  (>&2 echo -e "$(date +%T) \e[31m[ERROR]\e[0m $1")
  exit 1;
}

###############################################################################
# Support for KBs with no schema. Generates a universal schema with
#   predicate  NoDomain  NoDomain
# for each predicate.
###############################################################################
function construct_schema() {
  cut -d' ' ${INPUT_FACTS} -f1 | sort -u | sed 's/$/ Universe Universe/g' \
          > ${INPUT_SCHEMA}
}

###############################################################################
# Constructs candidate rules by traversing the schema graph.
###############################################################################
function construct_candidate_rules() {
  sed_schema_path="\\\set schema_file $(pwd)/${INPUT_SCHEMA}"
  sed_rules_path="\\\set rules_file  $(pwd)/${INPUT_RULES_PREFIX}"
  sed -i "1d;2s:.*:${sed_schema_path}\n${sed_rules_path}:" op.sql

  echo "Is the following setup correct?"
  echo "PostgreSQL user: ${POSTGRESQL_USER}"
  echo "PostgreSQL database: ${POSTGRESQL_DB}"
  read -p "[y/n]" -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    read -p "PostgreSQL user: " POSTGRESQL_USER
    read -p "PostgreSQL database: " POSTGRESQL_DB
  fi
  ${POSTGRESQL_BIN:+$POSTGRESQL_BIN/}psql -h ${POSTGRESQL_HOST} \
    -U ${POSTGRESQL_USER} -d ${POSTGRESQL_DB} -f op.sql \
    || print_error_exit "PostgreSQL failure."
}

###############################################################################
# Maps facts triples to integer representation.
# Arguments:
#   $1: Input facts in string representation.
#   $2: Input KB Schema in string representation.
#   $3: Result facts in integer representation.
#   $4: Whether we need to map entities, default to true. The author's Freebase
#       dataset has every entity pre-mapped. Otherwise set it to true.
#   $5: Map of predicates to integers, default to predicates.map.
#   $6: Map of entities to integers, default to entities.map.
# Returns:
#   Mapped facts stored in output file specified by $3.
###############################################################################
function map_facts_to_int() {
  input_facts=$1
  input_schema=$2
  output_facts_map=${3:-"${input_facts}.map"}
  map_entities=${4:-true}
  predicates_map=${5:-$(dirname "$1")/predicates.map}
  entities_map=${6:-$(dirname "$1")/entities.map}

  # Maps predicates to integers.
  cut -d' ' -f1 ${input_schema} | \
  sort -u | awk '{printf("%s %d\n", $0, NR)}' > ${predicates_map}

  if [ "${map_entities}" = true ]; then
    # Maps entities to integers.
    (cut -d' ' -f2 ${input_facts}; cut -d' ' -f3 ${input_facts}) | \
    sort -u | awk '{printf("%s %d\n", $0, NR)}' > ${entities_map}
  fi

  # Replaces strings by integers.
  awk 'FNR==NR{a[$1]=$2;next}{if (a[$1]) print a[$1],$2,$3}' \
  ${predicates_map} ${input_facts} > ${output_facts_map}

  if [ "${map_entities}" = true ]; then
    # Replaces entities by integers.
    awk 'FNR==NR{a[$1]=$2;next}{print $1,a[$2],a[$3]}' \
    ${entities_map} ${output_facts_map} > ${output_facts_map}.tmp
    mv ${output_facts_map}.tmp ${output_facts_map}
  fi
}


###############################################################################
# Maps rules tuples to integer representation.
# Arguments:
#   $1: Input rules in string representation.
#   $2: Input rule type (1-8).
#   $3: Result rules in integer representation.
#   $4: Map of predicates to integers, default to predicates.map.
# Returns:
#   Mapped rules stored in output file specified by $3.
###############################################################################
function map_rules_to_int() {
  input_rules=$1
  input_rules_type=$2
  output_rules_map=${3:-"${input_rules}.map"}
  predicates_map=${4:-$(dirname "$1")/predicates.map}
  rules_length=$(rule_type_to_length "${input_rules_type}")

  # Awk -- translating each column of rule to integer.
  awk_command='FNR==NR{a[$1]=$2;next}{print '"${input_rules_type}0000000+FNR"
  for i in $(seq 1 ${rules_length}); do
    awk_command+=',a[$'"$i]"
  done
  awk_command+='}'

  # Replaces strings by integers.
  awk "${awk_command}" ${predicates_map} ${input_rules} > ${output_rules_map}
}


###############################################################################
# Runs the partitioning algorithm.
# Arguments:
#   $1: Input facts in integer representation.
#   $2: Input rules in integer representation, first column assumed to be rule
#       ID.
#   $3: Output path.
# Returns:
#   Output partitions stored in directory specified by $3.
###############################################################################
function run_partitioning() {
  input_facts=$1
  input_rules=$2
  output_path=$3

  ${SPARK_PATH} \
    --class ${MAIN_CLASS} \
    --master local[${NCORES}] \
    --driver-memory ${DRIVER_MEMORY} \
    --executor-memory ${EXECUTOR_MEMORY} \
    ${JAR_PATH} partition \
    --input-facts "${input_facts}" \
    --input-rules "${input_rules}" \
    --output-dir "${output_path}" \
    --max-facts "${MAX_FACTS}" \
    --max-rules "${MAX_RULES}" \
    2>spark.log
}

###############################################################################
# Runs the main rule mining algorithm.
# Arguments:
#   $1: Input facts in integer representation.
#   $2: Input rules in integer representation.
#   $3: Input rules type (1-8).
#   $4: Functional constraint.
#   $5: Output directory to contain result rules and pruned rules.
#   $6: Output rules.
#   $7: Output of pruned rules.
# Returns:
#   Output rules stored in directory specified by $5.
###############################################################################
function run_mining() {
  input_facts=$1
  input_rules=$2
  input_rules_type=$3
  functional_constraint=$4
  output_path=$5
  output_rules=${6:-"${output_path}/rules"}
  output_prune=${7:-"${output_path}/prune"}
  prune=.prune$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)

  ${SPARK_PATH} \
    --class ${MAIN_CLASS} \
    --master local[${NCORES}] \
    --driver-memory ${DRIVER_MEMORY} \
    --executor-memory ${EXECUTOR_MEMORY} \
    ${JAR_PATH} learn \
    --input-facts "${input_facts}" \
    --input-rules "${input_rules}" \
    --output-dir "${output_path}" \
    --rule-type "${input_rules_type}" \
    --functional-constraint "${functional_constraint}" \
    >${prune} 2>>spark.log

  ### IMPORTANT ###
  # Remove pruned rules from output.
  sort ${prune} | uniq -c | awk '{printf("%d %d\n",$2,$1)}' > ${output_prune}
  cat ${output_path}/part-* | sort -k1,1 > ${output_path}/rules.all
  join -v1 ${output_path}/rules.all ${output_prune} \
      > ${output_rules}
}

###############################################################################
# Applies rules for knowledge expansion.
# Arguments:
#   $1: Input facts in integer representation.
#   $2: Input rules in integer representation.
#   $3: Input rules type (1-8).
#   $4: Output directory.
# Returns:
#   Output facts stored in directory specified by $4.
###############################################################################
function run_inference() {
  input_facts=$1
  input_rules=$2
  input_rules_type=$3
  output_path=$4

  ${SPARK_PATH} \
    --class ${MAIN_CLASS} \
    --master local[${NCORES}] \
    --driver-memory ${DRIVER_MEMORY} \
    --executor-memory ${EXECUTOR_MEMORY} \
    ${JAR_PATH} infer \
    --input-facts "${input_facts}" \
    --input-rules "${input_rules}" \
    --output-dir "${output_path}" \
    --rule-type "${input_rules_type}" --functional-constraint 100 \
    2>>spark.log
}

###############################################################################
# Format rules for output.
# Arguments:
#   $1: Initial rules in integer representation.
#   $2: Rules generated by the mining algorithm.
#   $3: Rules pruned by the mining algorithm.
#   $4: Rule type (1-8).
#   $5: Path of output rules.
#   $6: Path of output pruned rules.
#   $7: Path of the map from predicates to integers, default predicates.map.
# Returns:
#   Output and pruned rules stored in files specified by $5 and $6.
###############################################################################
function reformat_rules() {
  initial_rules=$1
  input_rules=$2
  input_prune=$3
  input_rules_type=$4
  output_rules=$5
  output_prune=$6
  predicates_map=${7:-$(dirname "$1")/predicates.map}
  rules_length=$(rule_type_to_length "${input_rules_type}")

  mkdir -p $(dirname "${output_rules}")

  # Maps rule IDs to (h, b) triples.
  join --check-order ${initial_rules} ${input_rules} > ${input_rules}.join
  join --check-order ${initial_rules} ${input_prune} > ${input_prune}.join

  # Maps integers to strings.
  awk_command='FNR==NR{a[$2]=$1;next}{print $1'
  for i in $(seq 1 ${rules_length}); do
    awk_command+=',a[$'"$((i+1))]"
  done

  # $((rules_length+2)) -> support.
  # $((rules_length+3)) -> confidence.
  awk "${awk_command}"',$'"$((rules_length+2))"',$'"$((rules_length+3))}" \
    ${predicates_map} ${input_rules}.join > ${output_rules}
  awk "${awk_command}}" \
    ${predicates_map} ${input_prune}.join > ${output_prune}
}

###############################################################################
# Format facts for output.
# Arguments:
#   $1: Inferred facts.
#   $2: Result rules in string representation with support and confidence
#       scores.
#   $3: Output facts.
#   $4: Map of predicates to integers, default to predicates.map.
#   $5: Map of entities to integers, default to entities.map.
# Returns:
#   Inferred facts stored in files specified by $3, with lineage, support, and
#   confidence scores.
###############################################################################
function reformat_facts() {
  input_facts=$1
  input_rules=$2
  output_facts=$3
  predicates_map=$4
  entities_map=$5

  input_dir=$(dirname "${input_facts}")
  output_dir=$(dirname "${output_facts}")
  mkdir -p "${output_dir}"

  # Map rule columns:
  awk -F'[ :]' 'FNR==NR{a[$2]=$1;next}{print a[$1],$2,$3,$4}' \
    "${predicates_map}" "${input_facts}" > "${input_dir}/facts.map"
  if [ "${MAP_ENTITIES}" = true ]; then
    awk -F'[ :]' 'FNR==NR{a[$2]=$1;next}{print $1,a[$2],a[$3],$4}' \
      ${entities_map} "${input_dir}/facts.map" > "${input_dir}/facts.map.tmp"
    mv "${input_dir}/facts.map.tmp" "${input_dir}/facts.map"
  fi
  join --check-order "${input_dir}/facts.map" "${input_rules}" -1 4 -2 1 | \
    cut -d' ' -f2- > "${output_facts}"
}

###############################################################################
# Maps rule type to length.
# Arguments:
#   $1: Rule type.
# Returns:
#   Rule length.
###############################################################################
function rule_type_to_length() {
  input_rule_type=$1

  if [ "${input_rule_type}" -lt 1 -o "${input_rule_type}" -gt 8 ]; then
    (>&2 echo "[ERROR] Invalid rule type ${input_rule_type}.")
    exit 1
  fi

  # Maps rule type to rule length.
  rule_type_length_array=(2 2 3 3 3 3 4 5)
  echo "${rule_type_length_array[$((input_rule_type-1))]}"
}

run
