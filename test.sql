SELECT `work_with`.`id` AS `work_with.id`
FROM   work_with
JOIN   `client` AS `work_with_client`
ON     `work_with`.`client` = `work_with_client`.`id`
JOIN   `company` AS `client_company`
ON     `client`.`company` = `client_company`.`id`
JOIN   `user` AS `work_with_user`
ON     `work_with`.`user` = `work_with_user`.`id`
JOIN   `company` AS `user_company`
ON     `user`.`company` = `user_company`.`id`
