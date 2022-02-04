# Query template
class Qtemplate:

    @staticmethod
    def fetch_all():
        return """SELECT 
                        *
                    FROM 
                        { this_table }"""
    
    @staticmethod
    def date_range_between():
        return """SELECT
                        *
                    FROM
                        { this_table }
                    WHERE
                        { col_name }
                    BETWEEN
                        '{{ start_date }}'
                    AND
                        '{{ end_date }}'
                    """