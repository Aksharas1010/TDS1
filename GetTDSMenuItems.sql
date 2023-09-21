CREATE PROCEDURE GetTDSMenuItems
AS
BEGIN
    -- Retrieve menu items from the MenuItems table
    SELECT
        menu_id,
        menu_name,
        parent_menu_id,
        menu_order,
        menu_url
    FROM
        tbl_TDSMenu
    WHERE
        is_active = 1 -- You can add additional conditions if needed
    ORDER BY
        parent_menu_id, menu_order;
END;
