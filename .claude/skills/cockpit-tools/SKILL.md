```markdown
# cockpit-tools Development Patterns

> Auto-generated skill from repository analysis

## Overview
This skill teaches the development patterns and conventions used in the `cockpit-tools` Rust repository. You'll learn how to structure files, write imports and exports, and follow the project's coding style. While no explicit workflows were detected, this guide provides practical conventions and suggestions for effective development and testing.

## Coding Conventions

### File Naming
- Use **camelCase** for file names.
  - **Example:** `myUtility.rs`, `dataParser.rs`

### Import Style
- Use **relative imports** within the codebase.
  - **Example:**
    ```rust
    mod utils;
    use crate::helpers::stringTools;
    ```

### Export Style
- Use **named exports** for modules and functions.
  - **Example:**
    ```rust
    pub fn parse_data(input: &str) -> Result<Data, Error> {
        // function body
    }
    ```

### Commit Patterns
- Commit messages are **freeform** with no strict prefixes.
- Average commit message length: **~40 characters**.
  - **Example:**  
    ```
    Fix parsing error in data loader
    ```

## Workflows

### Adding a New Module
**Trigger:** When introducing new functionality.
**Command:** `/add-module`

1. Create a new file using camelCase (e.g., `dataParser.rs`).
2. Implement your module logic.
3. Use `pub` to export functions or structs as needed.
4. Add a relative import in the main file or parent module.
    ```rust
    mod dataParser;
    use crate::dataParser::parse_data;
    ```
5. Write corresponding tests in a `*.test.rs` file.

### Writing and Running Tests
**Trigger:** When adding or updating code.
**Command:** `/run-tests`

1. Create a test file named with the pattern `*.test.rs` (e.g., `dataParser.test.rs`).
2. Write tests using Rust's built-in test framework.
    ```rust
    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_data_valid() {
            let input = "example";
            let result = parse_data(input);
            assert!(result.is_ok());
        }
    }
    ```
3. Run tests with:
    ```
    cargo test
    ```

## Testing Patterns

- Test files follow the pattern: `*.test.rs`
- Testing framework is **unknown**, but Rust's built-in test framework is recommended.
- Place tests in the same directory as the module or in a dedicated `tests/` folder.
- Example test structure:
    ```rust
    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_functionality() {
            // test code here
        }
    }
    ```

## Commands
| Command       | Purpose                                 |
|---------------|-----------------------------------------|
| /add-module   | Scaffold a new module with conventions  |
| /run-tests    | Run all tests in the repository         |
```
