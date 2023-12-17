DO $$
DECLARE
    counter INT := 1;
BEGIN
    WHILE counter <= 5 LOOP
        INSERT INTO users (username, first_name, last_name)
        VALUES (
            'test-username' || counter,
            'test-First' || counter,
            'test-Last' || counter
        );

        counter := counter + 1;
    END LOOP;
END $$;