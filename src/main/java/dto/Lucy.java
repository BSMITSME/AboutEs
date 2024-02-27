package dto;

public class Lucy {
    private String kw_docid;
    private String wc_sitename;
    private String au_domain;
    private int in_date;
    private String au_url;
    private String an_title;

    // getter
    public String get_kw_docid(){
        return kw_docid;
    }
    public String get_wc_sitename(){
        return wc_sitename;
    }
    public String get_au_domain(){
        return au_domain;
    }
    public int get_in_date(){
        return in_date;
    }
    public String get_au_url() {
        return au_url;

    }
    public String get_an_title(){
        return an_title;
    }

    // setter
    public void set_kw_docid(String docid){
        this.kw_docid = docid;
    }
    public void set_wc_sitename(String sitename){
        this.wc_sitename = sitename;
    }
    public void set_au_domain(String domain){
        this.au_domain = domain;
    }
    public void set_in_date(int date){
        this.in_date = date;
    }
    public void set_au_url(String url){
        this.au_url = url;
    }
    public void set_an_title(String title){
        this.an_title = title;
    }
}
