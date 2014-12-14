/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.Doc;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;

class HidingDocWrapper extends HidingWrapper implements Doc {
    
    public HidingDocWrapper(Doc doc, Map mapWrappers) {
        super(doc, mapWrappers);
    }

    private Doc _getDoc() {
        return (Doc)getWrappedObject();
    }

    public String commentText() {
        return _getDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getDoc().firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getDoc().inlineTags());
    }

    public boolean isClass() {
        return _getDoc().isClass();
    }

    public boolean isConstructor() {
        return _getDoc().isConstructor();
    }

    public boolean isError() {
        return _getDoc().isError();
    }

    public boolean isException() {
        return _getDoc().isException();
    }

    public boolean isField() {
        return _getDoc().isField();
    }

    public boolean isIncluded() {
        return _getDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getDoc().isInterface();
    }

    public boolean isMethod() {
        return _getDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getDoc().isOrdinaryClass();
    }

    public String name() {
        return _getDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getDoc().setRawCommentText(szText);
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getDoc().tags(szTagName));
    }
  
    public SourcePosition position() {
        return _getDoc().position();
    }

    public boolean isAnnotationType() {
        return _getDoc().isAnnotationType();
    }

    public boolean isEnum() {
        return _getDoc().isEnum();
    }

    public boolean isAnnotationTypeElement() {
        return _getDoc().isAnnotationTypeElement();
    }

    public boolean isEnumConstant() {
        return _getDoc().isEnumConstant();
    }
}
